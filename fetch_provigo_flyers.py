"""
For every Provigo store in data/provigo/stores.json:
  1. Fetch the list of active publications from:
       GET /flyerkit/publications/provigo
         ?languages[]=en&locale=en&access_token=...&store_code=<code>
  2. Merge new publications into data/provigo/store_flyers.json
     (keyed by store_code; deduped by publication ID within each store list)
  3. For each newly-seen publication ID (no existing file in data/provigo/flyers/):
       GET /flyerkit/publication/<id>/products
         ?display_type=all&locale=en&access_token=...
     Save products  →  data/provigo/flyers/<id>.json
     Save the fetch URL alongside each result so we can test whether the
     URL stays live after the flyer expires.

Safe to re-run weekly: already-downloaded flyer files are skipped and
existing store→flyer mappings are extended, not replaced.

Logging (files are dated to the run date):
  logs/provigo/<date>_verbose.log  — full output, mirrors the terminal
  logs/provigo/<date>_summary.log  — one-page run summary
"""

import json
import os
import time
from datetime import datetime, timezone

import requests

ACCESS_TOKEN = "31c52dc6a419dc10959261a5a210fccf"
PUBS_URL = "https://dam.flippenterprise.net/flyerkit/publications/provigo"
PRODUCTS_URL = "https://dam.flippenterprise.net/flyerkit/publication/{pub_id}/products"

STORES_PATH = "data/provigo/stores.json"
STORE_FLYERS_PATH = "data/provigo/store_flyers.json"
FLYERS_DIR = "data/provigo/flyers"
LOG_DIR = "logs/provigo"

DELAY = 0.05

# Module-level log file handles — assigned in main() before any logging
_verbose = None
_summary = None


# ── logging ───────────────────────────────────────────────────────────────────

def log(msg: str = "") -> None:
    """Print to stdout and append to the verbose log."""
    print(msg)
    if _verbose:
        _verbose.write(msg + "\n")
        _verbose.flush()


def log_summary(msg: str = "") -> None:
    """Print to stdout, append to verbose log, and append to summary log."""
    log(msg)
    if _summary:
        _summary.write(msg + "\n")
        _summary.flush()


# ── helpers ───────────────────────────────────────────────────────────────────

def get(url: str, params: dict) -> list | dict | None:
    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.RequestException as exc:
        log(f"    [!] request error – {exc}")
        return None
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def save_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log(f"  Saved → {path}  ({os.path.getsize(path):,} bytes)")


# ── step 1: fetch publications per store ─────────────────────────────────────

def fetch_store_publications(store_code: str) -> list:
    data = get(
        PUBS_URL,
        params={
            "languages[]": "en",
            "locale": "en",
            "access_token": ACCESS_TOKEN,
            "store_code": store_code,
        },
    )
    if data is None:
        return []
    if isinstance(data, dict):
        data = data.get("flyers", data.get("publications", []))
    return data if isinstance(data, list) else []


# ── step 2: fetch products for a publication ──────────────────────────────────

def fetch_publication_products(pub_id: int | str) -> list:
    url = PRODUCTS_URL.format(pub_id=pub_id)
    data = get(
        url,
        params={
            "display_type": "all",
            "locale": "en",
            "access_token": ACCESS_TOKEN,
        },
    )
    return data if isinstance(data, list) else []


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    global _verbose, _summary

    run_start = datetime.now(timezone.utc)
    today = run_start.strftime("%Y-%m-%d")

    # ── Set up dated log files ────────────────────────────────────────────────
    os.makedirs(LOG_DIR, exist_ok=True)
    verbose_path = os.path.join(LOG_DIR, f"{today}_verbose.log")
    summary_path = os.path.join(LOG_DIR, f"{today}_summary.log")
    _verbose = open(verbose_path, "a")
    _summary = open(summary_path, "a")

    log_summary(f"=== Provigo Flyer Fetch — {today} ===")
    log_summary(f"Run started : {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log_summary()

    # Load stores
    with open(STORES_PATH) as f:
        stores = json.load(f)

    log(f"Loaded {len(stores)} stores from {STORES_PATH}\n")
    log_summary(f"Stores loaded: {len(stores)}")

    # Load existing store_flyers accumulator (may not exist on first run)
    if os.path.exists(STORE_FLYERS_PATH):
        with open(STORE_FLYERS_PATH) as f:
            store_flyers: dict[str, list] = json.load(f)
        existing_count = sum(len(v) for v in store_flyers.values())
        log(f"Loaded existing store_flyers.json ({existing_count} publication entries)\n")
    else:
        store_flyers = {}

    # ── Phase 1: map each store → its publications ────────────────────────────
    log("Phase 1: fetching publications per store…\n")

    all_pubs: dict[str, dict] = {}   # pub_id (str) → publication object (new this run)
    new_entries = 0

    for store_code, store_info in stores.items():
        name = store_info.get("name", store_code)
        pubs = fetch_store_publications(store_code)

        # Build set of IDs already recorded for this store
        existing_ids = {str(p.get("id", "")) for p in store_flyers.get(store_code, [])}

        new_pubs = [p for p in pubs if str(p.get("id", "")) not in existing_ids]
        if new_pubs:
            store_flyers.setdefault(store_code, []).extend(new_pubs)
            new_entries += len(new_pubs)

        for pub in new_pubs:
            pid = str(pub.get("id", ""))
            if pid and pid not in all_pubs:
                all_pubs[pid] = pub

        new_ids = [str(p.get("id", "?")) for p in new_pubs]
        log(f"  [{store_code}] {name} → {new_ids or 'no new flyers'}")
        time.sleep(DELAY)

    log(f"\n  {new_entries} new publication entry/entries across {len(stores)} stores")
    log(f"  {len(all_pubs)} unique new publication(s) to download")
    save_json(STORE_FLYERS_PATH, store_flyers)

    log_summary(f"New publication entries added to store_flyers.json: {new_entries}")
    log_summary(f"Unique new publications to download: {len(all_pubs)}")

    # ── Phase 2: fetch & save products for each unique new publication ─────────
    log("\nPhase 2: fetching products for each new publication…\n")

    os.makedirs(FLYERS_DIR, exist_ok=True)

    downloaded = 0
    skipped = 0
    errors = 0

    if not all_pubs:
        log("  Nothing new to download.")
    else:
        for pub_id, pub_meta in all_pubs.items():
            out_path = os.path.join(FLYERS_DIR, f"{pub_id}.json")

            if os.path.exists(out_path):
                log(f"  [{pub_id}] already downloaded — skipping")
                skipped += 1
                continue

            name       = pub_meta.get("name", pub_id)
            valid_from = pub_meta.get("valid_from", pub_meta.get("start_date", "?"))
            valid_to   = pub_meta.get("valid_to",   pub_meta.get("end_date",   "?"))

            products_url = (
                f"https://dam.flippenterprise.net/flyerkit/publication/{pub_id}/products"
                f"?display_type=all&locale=en&access_token={ACCESS_TOKEN}"
            )

            log(f"  [{pub_id}] {name}  ({valid_from} → {valid_to})")

            products = fetch_publication_products(pub_id)

            if not products:
                log(f"    [!] no products returned — skipping")
                errors += 1
                continue

            log(f"    {len(products)} products")

            payload = {
                "fetched_on": today,
                "publication_id": pub_id,
                "publication_meta": pub_meta,
                "products_url": products_url,
                "products": products,
            }
            save_json(out_path, payload)
            downloaded += 1
            time.sleep(DELAY)

    run_end = datetime.now(timezone.utc)
    elapsed = run_end - run_start
    elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"

    log_summary()
    log_summary("── Phase 2 results ────────────────────────────────────────────────")
    log_summary(f"  Downloaded : {downloaded}")
    log_summary(f"  Skipped    : {skipped}  (file already existed)")
    log_summary(f"  Errors     : {errors}")
    log_summary()
    log_summary(f"Run completed: {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log_summary(f"Elapsed      : {elapsed_str}")
    log_summary()

    log(f"\nDone.  Verbose log → {verbose_path}")
    log(f"       Summary log → {summary_path}")

    _verbose.close()
    _summary.close()


if __name__ == "__main__":
    main()
