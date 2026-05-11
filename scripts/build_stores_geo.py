#!/usr/bin/env python3
"""
Build a unified stores_geo.parquet enriching every known store with
postal code, city, province, latitude, and longitude.

Sources
-------
Flipp chains  — raw_json in stores.parquet already contains full address data.
Metro chains  — no address data in stores.parquet; scraped from public store-finder
               pages (metro.ca, foodbasics.ca, superc.ca).
               Matching strategy: direct store-code match where ID spaces overlap;
               normalised-name fuzzy match otherwise.

Output
------
data/stores_geo.parquet  — one row per (chain, store_code)

Usage
-----
    python3 -m scripts.build_stores_geo          # all chains
    python3 -m scripts.build_stores_geo --flipp-only
    python3 -m scripts.build_stores_geo --metro-only
"""
from __future__ import annotations

import argparse
import html
import io
import json
import os
import re
import sys
import time
import unicodedata
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = Path("data")
OUT_PATH  = DATA_DIR / "stores_geo.parquet"
DELAY     = 0.3   # seconds between HTTP requests

GEONAMES_URL   = "https://download.geonames.org/export/zip/CA.zip"
GEONAMES_CACHE = DATA_DIR / "geonames_ca_cache.tsv"
OVERRIDES_PATH = DATA_DIR / "manual_geo_overrides.csv"

WEB_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

FLIPP_FOLDERS = [
    "loblaws", "nofrills", "provigo", "real_canadian_superstore", "maxi",
    "zehrs", "fortinos", "atlantic_superstore", "dominion",
    "independent_grocer", "independent_city_market", "freshmart",
    "sobeys", "safeway", "iga", "freshco", "foodland", "longos",
    "farm_boy", "walmart",
]

METRO_WEB_SOURCES: dict[str, str | None] = {
    "metro":       "https://www.metro.ca/en/find-shopping-store",
    "metro_qc":    "https://www.metro.ca/en/find-shopping-store",   # same page, ON+QC combined
    "food_basics": "https://www.foodbasics.ca/en/find-shopping-store",
    "super_c":     "https://www.superc.ca/en/find-shopping-store",
    "adonis":      None,  # site unreachable from container; falls back to null geo
}

# Output schema
GEO_SCHEMA = pa.schema([
    pa.field("chain",       pa.string()),
    pa.field("store_code",  pa.string()),
    pa.field("store_name",  pa.string()),
    pa.field("address",     pa.string()),
    pa.field("city",        pa.string()),
    pa.field("province",    pa.string()),
    pa.field("postal_code", pa.string()),
    pa.field("lat",         pa.float64()),
    pa.field("lon",         pa.float64()),
    pa.field("source_api",  pa.string()),   # "flipp" | "metro"
    pa.field("geo_source",  pa.string()),   # "raw_json" | "web_direct" | "web_name_match" | "fsa_centroid" | "none"
])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm(s: str) -> str:
    """Lower-case, strip accents, collapse whitespace, remove punctuation."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Remove common chain-name noise
    for noise in ("super c", "metro plus", "metro", "food basics", "foodbasics",
                  "no frills", "loblaws", "sobeys", "safeway", "walmart",
                  "inc", "ltee", "ltée", "limited", "grocery"):
        s = re.sub(rf"\b{noise}\b", "", s).strip()
    return re.sub(r"\s+", " ", s).strip()


def _best_name_match(
    db_name: str,
    web_stores: list[dict],
    used: set[str],
) -> dict | None:
    """Return the web store whose normalised name best overlaps with db_name."""
    norm_db = _norm(db_name)
    if not norm_db:
        return None

    db_tokens = set(norm_db.split())
    best_score = 0
    best = None

    for ws in web_stores:
        wid = ws["web_id"]
        if wid in used:
            continue
        norm_web = _norm(ws["name"])
        web_tokens = set(norm_web.split())
        if not web_tokens:
            continue
        # Jaccard over tokens
        inter = db_tokens & web_tokens
        union = db_tokens | web_tokens
        score = len(inter) / len(union) if union else 0.0
        if score > best_score:
            best_score = score
            best = ws

    # Only accept if overlap is meaningful (at least one shared token with min 3 chars)
    if best and best_score > 0.0:
        norm_web = _norm(best["name"])
        shared = {t for t in db_tokens & set(norm_web.split()) if len(t) >= 3}
        if shared:
            return best
    return None


# ── Web scraper ───────────────────────────────────────────────────────────────

def _scrape_metro_page(url: str) -> list[dict]:
    """Scrape a Metro-group HTML store-finder page.

    Returns a list of store dicts with keys:
        web_id, name, lat, lon, city, street, postal_code, province
    """
    try:
        r = requests.get(url, headers=WEB_HEADERS, timeout=20)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"  [!] Could not fetch {url}: {e}", file=sys.stderr)
        return []

    text = r.text
    li_blocks = re.findall(r'<li class="fs--box-shop"[^>]+>.*?</li>', text, re.S)

    stores = []
    for block in li_blocks:
        sid     = re.search(r'data-storeid="([^"]+)"', block)
        lat_m   = re.search(r'data-store-lat="([^"]+)"', block)
        lng_m   = re.search(r'data-store-lng="([^"]+)"', block)
        name_m  = re.search(r'data-store-name="([^"]+)"', block)
        city_m  = re.search(r'data-city="([^"]+)"', block)
        street_m = re.search(r'data-street="([^"]+)"', block)
        postal_m = re.search(r'address--postalCode">([^<]+)<', block)
        prov_m   = re.search(r'address--provinceCode">([^<]+)<', block)

        if not sid:
            continue

        # Parse city/province/postal from data-city="Cornwall ON K6J 4P5"
        city_full = html.unescape(city_m.group(1)).strip() if city_m else ""
        city_parsed = prov_parsed = postal_parsed = ""
        m = re.match(r"^(.*?)\s+([A-Z]{2})\s+([A-Z]\d[A-Z]\s*\d[A-Z]\d)\s*$", city_full)
        if m:
            city_parsed   = m.group(1).strip()
            prov_parsed   = m.group(2)
            postal_parsed = m.group(3).replace(" ", "")
        else:
            city_parsed = city_full

        # postal/province from explicit spans override data-city parse
        if postal_m:
            postal_parsed = postal_m.group(1).strip().replace(" ", "")
        if prov_m:
            prov_parsed = prov_m.group(1).strip()

        try:
            lat = float(lat_m.group(1)) if lat_m else None
            lon = float(lng_m.group(1)) if lng_m else None
        except ValueError:
            lat = lon = None

        stores.append({
            "web_id":      sid.group(1),
            "name":        html.unescape(name_m.group(1)).strip() if name_m else "",
            "lat":         lat,
            "lon":         lon,
            "city":        html.unescape(city_parsed).strip() if city_parsed else (street_m.group(1) if street_m else ""),
            "street":      html.unescape(street_m.group(1)).strip() if street_m else "",
            "postal_code": postal_parsed,
            "province":    prov_parsed,
        })

    return stores


# ── Flipp extraction ──────────────────────────────────────────────────────────

def extract_flipp_geo(verbose: bool = True) -> list[dict]:
    """Read all Flipp stores.parquet files and return geo-enriched rows."""
    rows_out: list[dict] = []

    for folder in FLIPP_FOLDERS:
        path = DATA_DIR / folder / "stores.parquet"
        if not path.exists():
            if verbose:
                print(f"  [skip] {folder}: no stores.parquet")
            continue
        try:
            t = pq.read_table(str(path))
        except Exception as e:
            print(f"  [!] {folder}: could not read parquet – {e}", file=sys.stderr)
            continue

        d = t.to_pydict()
        codes = d.get("store_code", [])
        names = d.get("store_name", [""] * len(codes))
        raws  = d.get("raw_json",   ["{}"] * len(codes))
        count = 0

        for code, name, raw in zip(codes, names, raws):
            try:
                obj = json.loads(raw) if raw else {}
            except ValueError:
                obj = {}

            lat = obj.get("latitude")
            lon = obj.get("longitude")
            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (TypeError, ValueError):
                lat = lon = None

            postal = (obj.get("postal_code") or "").replace(" ", "").upper()

            rows_out.append({
                "chain":       folder,
                "store_code":  str(code),
                "store_name":  name or obj.get("name", ""),
                "address":     obj.get("address", ""),
                "city":        obj.get("city", ""),
                "province":    obj.get("province", ""),
                "postal_code": postal,
                "lat":         lat,
                "lon":         lon,
                "source_api":  "flipp",
                "geo_source":  "raw_json",
            })
            count += 1

        if verbose:
            print(f"  {folder}: {count} stores")

    return rows_out


# ── Metro extraction ──────────────────────────────────────────────────────────

def extract_metro_geo(verbose: bool = True) -> list[dict]:
    """Scrape Metro-group web store finders and match against stores.parquet."""
    rows_out: list[dict] = []

    # Cache web pages (metro.ca covers both metro + metro_qc)
    web_cache: dict[str, list[dict]] = {}

    for folder, web_url in METRO_WEB_SOURCES.items():
        path = DATA_DIR / folder / "stores.parquet"
        if not path.exists():
            if verbose:
                print(f"  [skip] {folder}: no stores.parquet")
            continue
        try:
            t = pq.read_table(str(path))
        except Exception as e:
            print(f"  [!] {folder}: could not read parquet – {e}", file=sys.stderr)
            continue

        d = t.to_pydict()
        codes = d.get("store_code", [])
        names = d.get("store_name", [""] * len(codes))
        raws  = d.get("raw_json",   ["{}"] * len(codes))

        # Fetch or retrieve cached web store list
        web_stores: list[dict] = []
        if web_url:
            if web_url not in web_cache:
                if verbose:
                    print(f"  Scraping {web_url} …")
                web_cache[web_url] = _scrape_metro_page(web_url)
                time.sleep(DELAY)
            web_stores = web_cache[web_url]
            if verbose:
                print(f"  {folder}: {len(codes)} DB stores, {len(web_stores)} web stores")

        # Build lookup dicts
        web_by_id   = {ws["web_id"]: ws for ws in web_stores}
        used_web_ids: set[str] = set()

        # First pass: direct ID match
        direct_matches = 0
        unmatched_codes: list[tuple[str, str, str]] = []

        for code, name, raw in zip(codes, names, raws):
            code_str = str(code)
            ws = web_by_id.get(code_str)
            if ws:
                used_web_ids.add(code_str)
                direct_matches += 1
                rows_out.append({
                    "chain":       folder,
                    "store_code":  code_str,
                    "store_name":  name,
                    "address":     ws["street"],
                    "city":        ws["city"],
                    "province":    ws["province"],
                    "postal_code": ws["postal_code"],
                    "lat":         ws["lat"],
                    "lon":         ws["lon"],
                    "source_api":  "metro",
                    "geo_source":  "web_direct",
                })
            else:
                unmatched_codes.append((code_str, name, raw))

        if verbose and web_stores:
            print(f"    direct ID matches: {direct_matches}")

        # Second pass: name-based fuzzy match for unmatched stores
        name_matches = 0
        no_match = 0

        for code_str, name, raw in unmatched_codes:
            ws = _best_name_match(name, web_stores, used_web_ids)
            if ws:
                used_web_ids.add(ws["web_id"])
                name_matches += 1
                rows_out.append({
                    "chain":       folder,
                    "store_code":  code_str,
                    "store_name":  name,
                    "address":     ws["street"],
                    "city":        ws["city"],
                    "province":    ws["province"],
                    "postal_code": ws["postal_code"],
                    "lat":         ws["lat"],
                    "lon":         ws["lon"],
                    "source_api":  "metro",
                    "geo_source":  "web_name_match",
                })
            else:
                no_match += 1
                rows_out.append({
                    "chain":       folder,
                    "store_code":  code_str,
                    "store_name":  name,
                    "address":     "",
                    "city":        "",
                    "province":    "",
                    "postal_code": "",
                    "lat":         None,
                    "lon":         None,
                    "source_api":  "metro",
                    "geo_source":  "none",
                })

        if verbose:
            if web_stores:
                print(f"    name matches: {name_matches}  unmatched: {no_match}")
            else:
                print(f"  {folder}: {len(codes)} stores, no web source available – geo=null")

    return rows_out


# ── Manual overrides ────────────────────────────────────────────────────────

def _load_manual_overrides() -> dict[tuple[str, str], dict]:
    """Load data/manual_geo_overrides.csv → {(chain, store_code): {postal_code, lat, lon}}.

    Only rows where postal_code is non-empty are returned.
    lat/lon columns are optional — leave blank to rely on FSA centroid fallback.
    """
    if not OVERRIDES_PATH.exists():
        return {}

    import csv
    overrides: dict[tuple[str, str], dict] = {}
    with open(OVERRIDES_PATH, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            postal = row.get("postal_code", "").strip().replace(" ", "").upper()
            if not postal:
                continue
            lat_s = row.get("lat", "").strip()
            lon_s = row.get("lon", "").strip()
            try:
                lat = float(lat_s) if lat_s else None
                lon = float(lon_s) if lon_s else None
            except ValueError:
                lat = lon = None
            overrides[(row["chain"].strip(), row["store_code"].strip())] = {
                "postal_code": postal,
                "lat": lat,
                "lon": lon,
                "address": row.get("found_address", "").strip(),
            }
    return overrides


# ── FSA centroid lookup ──────────────────────────────────────────────────────

def _load_fsa_centroids() -> dict[str, tuple[float, float]]:
    """Return {FSA: (mean_lat, mean_lon)} from GeoNames Canada postal codes.

    Downloads once and caches to data/geonames_ca_cache.tsv.
    """
    if GEONAMES_CACHE.exists():
        tsv_bytes = GEONAMES_CACHE.read_bytes()
    else:
        print(f"  Downloading GeoNames Canada postal codes from {GEONAMES_URL} …")
        try:
            r = requests.get(GEONAMES_URL, timeout=60)
            r.raise_for_status()
        except requests.RequestException as e:
            print(f"  [!] Could not download GeoNames data: {e}", file=sys.stderr)
            return {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            tsv_bytes = zf.read("CA.txt")
        GEONAMES_CACHE.parent.mkdir(parents=True, exist_ok=True)
        GEONAMES_CACHE.write_bytes(tsv_bytes)
        print(f"  Cached → {GEONAMES_CACHE}")

    # Columns: country, postal_code, place_name, ..., latitude(9), longitude(10), ...
    # Postal codes look like "A1A 1A1"; FSA = first 3 chars after stripping the space.
    fsa_lats: dict[str, list[float]] = defaultdict(list)
    fsa_lons: dict[str, list[float]] = defaultdict(list)

    for line in tsv_bytes.decode("utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) < 11:
            continue
        fsa = parts[1].replace(" ", "")[:3].upper()
        if not fsa:
            continue
        try:
            lat = float(parts[9])
            lon = float(parts[10])
        except ValueError:
            continue
        fsa_lats[fsa].append(lat)
        fsa_lons[fsa].append(lon)

    return {
        fsa: (sum(lats) / len(lats), sum(fsa_lons[fsa]) / len(fsa_lons[fsa]))
        for fsa, lats in fsa_lats.items()
    }


# ── Write output ──────────────────────────────────────────────────────────────

def build_stores_geo(flipp: bool = True, metro: bool = True) -> None:
    """Build and save data/stores_geo.parquet."""
    rows: list[dict] = []

    if flipp:
        print("── Flipp chains ──────────────────────────────────────")
        rows.extend(extract_flipp_geo())

    if metro:
        print("── Metro chains ──────────────────────────────────────")
        rows.extend(extract_metro_geo())

    if not rows:
        print("No rows produced — nothing to write.", file=sys.stderr)
        return

    # Apply manual overrides first
    print("── Manual geo overrides ───────────────────────────────")
    overrides = _load_manual_overrides()
    if overrides:
        applied = 0
        for row in rows:
            key = (row["chain"], row["store_code"])
            if key in overrides:
                ov = overrides[key]
                row["postal_code"] = ov["postal_code"]
                if ov.get("address"):
                    row["address"] = ov["address"]
                if ov["lat"] is not None:
                    row["lat"] = ov["lat"]
                    row["lon"] = ov["lon"]
                    row["geo_source"] = "manual"
                else:
                    row["geo_source"] = "manual"  # lat/lon will come from FSA centroid
                applied += 1
        print(f"  Applied {applied} override(s) from {OVERRIDES_PATH}")
    else:
        print(f"  {OVERRIDES_PATH} not found or empty — skipping")

    # Backfill lat/lon using FSA centroids for stores that have a postal code
    print("── FSA centroid enrichment ────────────────────────────")
    centroids = _load_fsa_centroids()
    enriched = 0
    for row in rows:
        if row.get("lat") is None and row.get("postal_code"):
            fsa = row["postal_code"][:3].upper()
            if fsa in centroids:
                row["lat"], row["lon"] = centroids[fsa]
                row["geo_source"] = "fsa_centroid"
                enriched += 1
    print(f"  Enriched {enriched:,} stores ({len(centroids)} FSAs loaded)")

    # Build PyArrow table
    def _col(key: str) -> list:
        return [r.get(key) for r in rows]

    table = pa.table(
        {
            "chain":       pa.array(_col("chain"),       type=pa.string()),
            "store_code":  pa.array(_col("store_code"),  type=pa.string()),
            "store_name":  pa.array(_col("store_name"),  type=pa.string()),
            "address":     pa.array(_col("address"),     type=pa.string()),
            "city":        pa.array(_col("city"),        type=pa.string()),
            "province":    pa.array(_col("province"),    type=pa.string()),
            "postal_code": pa.array(_col("postal_code"), type=pa.string()),
            "lat":         pa.array(_col("lat"),         type=pa.float64()),
            "lon":         pa.array(_col("lon"),         type=pa.float64()),
            "source_api":  pa.array(_col("source_api"),  type=pa.string()),
            "geo_source":  pa.array(_col("geo_source"),  type=pa.string()),
        },
        schema=GEO_SCHEMA,
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, str(OUT_PATH))

    # Summary
    total = len(rows)
    has_postal = sum(1 for r in rows if r.get("postal_code"))
    has_geo    = sum(1 for r in rows if r.get("lat") is not None)
    geo_none   = sum(1 for r in rows if r.get("geo_source") == "none")
    geo_manual = sum(1 for r in rows if r.get("geo_source") == "manual")
    print(f"\n✓ Wrote {total:,} stores → {OUT_PATH}")
    print(f"  postal_code coverage : {has_postal:,} / {total:,} ({100*has_postal//total}%)")
    print(f"  lat/lon coverage     : {has_geo:,} / {total:,} ({100*has_geo//total}%)")
    if geo_manual:
        print(f"  manual overrides     : {geo_manual:,} stores")
    print(f"  no geo match         : {geo_none:,} stores")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build data/stores_geo.parquet")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--flipp-only", action="store_true", help="Only process Flipp chains")
    g.add_argument("--metro-only", action="store_true", help="Only process Metro chains")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build_stores_geo(
        flipp=not args.metro_only,
        metro=not args.flipp_only,
    )
