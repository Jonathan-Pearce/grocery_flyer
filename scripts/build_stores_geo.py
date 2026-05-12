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
import re
import sys
import time
import unicodedata
import zipfile
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

# ── Config ────────────────────────────────────────────────────────────────────

DATA_DIR = Path("data")
OUT_PATH  = DATA_DIR / "stores_geo.parquet"
DELAY     = 0.3   # seconds between HTTP requests

GEONAMES_URL     = "https://download.geonames.org/export/zip/CA.zip"
GEONAMES_CACHE   = DATA_DIR / "geonames_ca_cache.tsv"
OVERRIDES_PATH   = DATA_DIR / "manual_geo_overrides.csv"
NOMINATIM_CACHE  = DATA_DIR / "nominatim_cache.json"
NOMINATIM_URL    = "https://nominatim.openstreetmap.org/search"
NOMINATIM_DELAY  = 1.5   # seconds — Nominatim requires ≥ 1 s between requests; 1.5 s gives headroom
NOMINATIM_UA     = "grocery-flyer-project/1.0 (github.com/Jonathan-Pearce/grocery_flyer)"

FLIPP_BASE = "https://dam.flippenterprise.net/flyerkit"

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

# Chains whose Flipp raw_json lacks lat/lon — enriched via /flyerkit/stores?postal_code=
# (slug, access_token) per folder
FLIPP_POSTAL_SOURCES: dict[str, tuple[str, str]] = {
    "sobeys":                  ("sobeys",                 "afbc75b4e335236182ac2fba092a0d4a"),
    "safeway":                 ("safewaycanada",           "41073822c1e3a003da36de785443fa0f"),
    "iga":                     ("igaquebec",               "692be3f8ba9e9247dc13d064cb89e7f9"),
    "freshco":                 ("freshco",                 "881f0b9feea3693a704952a69b2a037a"),
    "foodland":                ("foodland",                "07ca28af93a0585f05575bf41ce92a6d"),
    "longos":                  ("longos",                  "5b4ad9bb0148449f25dbb0b76b976c1b"),
    "farm_boy":                ("farmboy",                 "633f9e9fe2eae3e7b4a811dd9690ac4b"),
    "walmart":                 ("walmartcanada",           "92bcff5f7d07c3aaa4b33e2c048d7728"),
    "independent_grocer":      ("yourindependentgrocer",   "fa31161a375478b68b2ec0f8f8edd65a"),
    "independent_city_market": ("independentcitymarket",   "a30dee18036c0131c522b0fd12632b7d"),
    "freshmart":               ("freshmart",               "32520249c4e20e14b33e5d45d084cb53"),
}

METRO_WEB_SOURCES: dict[str, str | None] = {
    "metro":       "https://www.metro.ca/en/find-shopping-store",
    "metro_qc":    "https://www.metro.ca/en/find-shopping-store",   # same page, ON+QC combined
    "food_basics": "https://www.foodbasics.ca/en/find-shopping-store",
    "super_c":     "https://www.superc.ca/en/find-shopping-store",
    "adonis":      None,  # site unreachable from container; falls back to null geo
}

# Province filter: only match web stores from the chain's home province.
# metro.ca returns both ON and QC stores; without filtering, store codes can
# collide across provinces (e.g. QC web ID 377 != ON DB code 377).
METRO_CHAIN_PROVINCE: dict[str, str | None] = {
    "metro":       "ON",
    "metro_qc":    "QC",
    "food_basics": "ON",
    "super_c":     "QC",
    "adonis":      None,
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
    pa.field("geo_source",  pa.string()),   # "raw_json" | "flipp_api" | "nominatim" | "web_direct" | "web_name_match" | "postal_centroid" | "fsa_centroid" | "manual" | "none"
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
    best_score = 0.0
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

        # Filter web stores to this chain's expected province to avoid ID collisions
        # (metro.ca returns both ON and QC stores in a single page)
        expected_prov = METRO_CHAIN_PROVINCE.get(folder)
        if expected_prov and web_stores:
            web_stores = [ws for ws in web_stores if ws.get("province") == expected_prov]

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


# ── Flipp API postal enrichment ──────────────────────────────────────────

def _enrich_flipp_postal(rows: list[dict], verbose: bool = True) -> int:
    """Enrich rows missing lat/lon using the Flipp /stores?postal_code= endpoint.

    For each chain in FLIPP_POSTAL_SOURCES, queries every unique postal code
    and accumulates {merchant_store_code: (lat, lon)}, then matches back to rows.
    Returns the number of rows newly enriched.
    """
    from collections import defaultdict as _defaultdict

    # Group rows needing enrichment by chain
    by_chain: dict[str, list[dict]] = _defaultdict(list)
    for row in rows:
        folder = row.get("chain", "")
        if folder in FLIPP_POSTAL_SOURCES and row.get("lat") is None and row.get("postal_code"):
            by_chain[folder].append(row)

    if not by_chain:
        return 0

    total_enriched = 0

    for folder, chain_rows in by_chain.items():
        slug, token = FLIPP_POSTAL_SOURCES[folder]

        # Unique postal codes for this chain (sorted for determinism)
        unique_postals = sorted({r["postal_code"] for r in chain_rows})

        # Accumulate {merchant_store_code: (lat, lon)} from all API responses
        store_latlon: dict[str, tuple[float, float]] = {}
        errors = 0

        for postal in unique_postals:
            try:
                resp = requests.get(
                    f"{FLIPP_BASE}/stores/{slug}",
                    params={"postal_code": postal, "locale": "en", "access_token": token},
                    timeout=10,
                )
                if resp.status_code == 200:
                    for s in resp.json() if isinstance(resp.json(), list) else []:
                        code = str(s.get("merchant_store_code", ""))
                        lat  = s.get("latitude")
                        lon  = s.get("longitude")
                        if code and lat is not None and lon is not None:
                            try:
                                store_latlon[code] = (float(lat), float(lon))
                            except (TypeError, ValueError):
                                pass
            except requests.RequestException:
                errors += 1
            time.sleep(DELAY)

        # Apply matches back to rows
        enriched = 0
        for row in chain_rows:
            if row["store_code"] in store_latlon:
                row["lat"], row["lon"] = store_latlon[row["store_code"]]
                row["geo_source"] = "flipp_api"
                enriched += 1

        total_enriched += enriched
        if verbose:
            missed = len(chain_rows) - enriched
            print(
                f"  {folder}: {enriched}/{len(chain_rows)} geocoded "
                f"({len(unique_postals)} postal queries, {missed} unmatched, {errors} errors)"
            )

    return total_enriched


# ── Nominatim address geocoding ─────────────────────────────────────────────

def _nominatim_geocode_batch(rows: list[dict], verbose: bool = True) -> int:
    """Geocode rows that have an address but no lat/lon via Nominatim.

    Results are persisted to data/nominatim_cache.json so re-runs are instant.
    Returns the number of stores newly geocoded.
    """
    # Load existing cache
    cache: dict[str, list] = {}
    if NOMINATIM_CACHE.exists():
        try:
            cache = json.loads(NOMINATIM_CACHE.read_text("utf-8"))
        except (ValueError, OSError):
            cache = {}

    needs = [
        r for r in rows
        if r.get("lat") is None and r.get("address") and r.get("city")
    ]
    if not needs:
        return 0

    SAVE_INTERVAL = 50   # persist cache every N live requests

    enriched       = 0
    new_entries    = 0
    cache_hits     = 0

    def _save_cache() -> None:
        NOMINATIM_CACHE.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2), "utf-8"
        )

    for row in needs:
        key = f"{row['address']}, {row['city']}, {row.get('province', '')}, Canada"

        if key in cache:
            cached = cache[key]
            if cached[0] is not None:
                row["lat"]        = cached[0]
                row["lon"]        = cached[1]
                row["geo_source"] = "nominatim"
                enriched += 1
            cache_hits += 1
            continue

        # Live Nominatim request — exponential backoff on 429
        for attempt in range(5):
            try:
                params: dict[str, str | int] = {
                    "q": key,
                    "format": "json",
                    "limit": 1,
                    "countrycodes": "ca",
                }
                resp = requests.get(
                    NOMINATIM_URL,
                    params=params,
                    headers={"User-Agent": NOMINATIM_UA},
                    timeout=15,
                )
            except requests.RequestException as e:
                print(f"  [!] Nominatim error for '{key}': {e}", file=sys.stderr)
                cache[key] = [None, None]
                break

            if resp.status_code == 429:
                backoff = 30 * (2 ** attempt)  # 30s, 60s, 120s, 240s, 480s
                print(f"  [!] Nominatim rate-limited — waiting {backoff}s …", file=sys.stderr)
                time.sleep(backoff)
                continue

            try:
                resp.raise_for_status()
                results = resp.json()
            except (requests.RequestException, ValueError) as e:
                print(f"  [!] Nominatim error for '{key}': {e}", file=sys.stderr)
                cache[key] = [None, None]
                break

            if results:
                lat = float(results[0]["lat"])
                lon = float(results[0]["lon"])
                cache[key]        = [lat, lon]
                row["lat"]        = lat
                row["lon"]        = lon
                row["geo_source"] = "nominatim"
                enriched += 1
            else:
                cache[key] = [None, None]
            break

        new_entries += 1
        time.sleep(NOMINATIM_DELAY)

        # Incremental cache save so progress survives interruptions
        if new_entries % SAVE_INTERVAL == 0:
            _save_cache()
            if verbose:
                print(f"  … {new_entries:,} requests ({enriched:,} geocoded, {cache_hits:,} cache hits)")

    # Final save
    if new_entries > 0:
        _save_cache()

    if verbose:
        total_with_addr = len(needs)
        print(
            f"  geocoded {enriched:,} / {total_with_addr:,} address stores  "
            f"(cache hits: {cache_hits:,}  live requests: {new_entries:,})"
        )

    return enriched


# ── Postal / FSA centroid lookup ─────────────────────────────────────────────

def _load_postal_centroids() -> tuple[dict[str, tuple[float, float]], dict[str, tuple[float, float]]]:
    """Return (postal_dict, fsa_dict) from GeoNames Canada postal codes.

    postal_dict : {6-char postal code: (lat, lon)}  — single representative point per code
    fsa_dict    : {3-char FSA: (mean_lat, mean_lon)} — fallback when full code not present

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
            return {}, {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            tsv_bytes = zf.read("CA.txt")
        GEONAMES_CACHE.parent.mkdir(parents=True, exist_ok=True)
        GEONAMES_CACHE.write_bytes(tsv_bytes)
        print(f"  Cached → {GEONAMES_CACHE}")

    # Columns: country, postal_code, place_name, ..., latitude(9), longitude(10), ...
    # GeoNames postal codes look like "A1A 1A1"
    postal: dict[str, tuple[float, float]] = {}
    fsa_lats: dict[str, list[float]] = defaultdict(list)
    fsa_lons: dict[str, list[float]] = defaultdict(list)

    for line in tsv_bytes.decode("utf-8").splitlines():
        parts = line.split("\t")
        if len(parts) < 11:
            continue
        code = parts[1].replace(" ", "").upper()  # "A1A1A1"
        if not code:
            continue
        try:
            lat = float(parts[9])
            lon = float(parts[10])
        except ValueError:
            continue
        # Full 6-char postal code — take first occurrence (GeoNames has one row per code)
        if len(code) == 6 and code not in postal:
            postal[code] = (lat, lon)
        # FSA accumulator
        fsa = code[:3]
        fsa_lats[fsa].append(lat)
        fsa_lons[fsa].append(lon)

    fsa_dict = {
        k: (sum(lats) / len(lats), sum(fsa_lons[k]) / len(fsa_lons[k]))
        for k, lats in fsa_lats.items()
    }
    return postal, fsa_dict


# ── Write output ──────────────────────────────────────────────────────────────

def build_stores_geo(flipp: bool = True, metro: bool = True, nominatim: bool = True) -> None:
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

    # Flipp API postal enrichment (fast, no rate limit, uses existing credentials)
    print("── Flipp API postal enrichment ───────────────────────")
    _enrich_flipp_postal(rows)

    # Nominatim geocoding for any stores still missing (Flipp API misses, PO box addresses, etc.)
    if nominatim:
        print("── Nominatim address geocoding ────────────────────────")
        _nominatim_geocode_batch(rows)

    # Backfill lat/lon using postal/FSA centroids for stores still missing coordinates
    print("── Postal centroid enrichment ─────────────────────────")
    postal_centroids, fsa_centroids = _load_postal_centroids()
    postal_hits = fsa_hits = 0
    for row in rows:
        if row.get("lat") is not None or not row.get("postal_code"):
            continue
        code = row["postal_code"].replace(" ", "").upper()
        if len(code) == 6 and code in postal_centroids:
            row["lat"], row["lon"] = postal_centroids[code]
            row["geo_source"] = "postal_centroid"
            postal_hits += 1
        elif code[:3] in fsa_centroids:
            row["lat"], row["lon"] = fsa_centroids[code[:3]]
            row["geo_source"] = "fsa_centroid"
            fsa_hits += 1
    print(
        f"  Enriched {postal_hits + fsa_hits:,} stores "
        f"({postal_hits:,} full postal, {fsa_hits:,} FSA fallback — "
        f"{len(postal_centroids):,} codes / {len(fsa_centroids):,} FSAs loaded)"
    )

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
    p.add_argument(
        "--no-nominatim",
        action="store_true",
        help="Skip Nominatim geocoding (faster; uses FSA centroid for all stores)",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build_stores_geo(
        flipp=not args.metro_only,
        metro=not args.flipp_only,
        nominatim=not args.no_nominatim,
    )
