"""
Weekly flyer fetcher for all Loblaws portfolio brands.

For each brand that has a confirmed slug and a data/<folder>/stores.json:
  Phase 1 — fetch active publications per store, merge into
             data/<folder>/store_flyers.json  (append-only, no duplicates)
  Phase 2 — download products for each new publication ID into
             data/<folder>/flyers/<id>.json  (skip if file already exists)

Logs:
  logs/<folder>/<date>_verbose.log
  logs/<folder>/<date>_summary.log

Safe to re-run: existing data is never overwritten.
"""

import json
import os
import time

from flipp import (
    DELAY,
    LOBLAWS_PORTFOLIO,
    Brand,
    FlippLogger,
    fetch_publication_products,
    fetch_store_publications,
    now_utc,
    save_json,
)


def fetch_brand_flyers(brand: Brand, today: str) -> None:
    stores_path      = f"data/{brand.folder}/stores.json"
    store_flyers_path = f"data/{brand.folder}/store_flyers.json"
    flyers_dir        = f"data/{brand.folder}/flyers"
    log_dir           = f"logs/{brand.folder}"

    logger = FlippLogger(log_dir, today)
    log     = logger.log
    summary = logger.summary

    run_start = now_utc()
    summary(f"=== {brand.name} Flyer Fetch — {today} ===")
    summary(f"Run started : {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary()

    # ── Load stores ───────────────────────────────────────────────────────────
    with open(stores_path) as f:
        stores = json.load(f)

    log(f"Loaded {len(stores)} stores from {stores_path}\n")
    summary(f"Stores loaded: {len(stores)}")

    # ── Load existing store_flyers accumulator ────────────────────────────────
    if os.path.exists(store_flyers_path):
        with open(store_flyers_path) as f:
            store_flyers: dict[str, list] = json.load(f)
        log(f"Loaded existing store_flyers.json "
            f"({sum(len(v) for v in store_flyers.values())} publication entries)\n")
    else:
        store_flyers = {}

    # ── Phase 1: store → publications ─────────────────────────────────────────
    log("Phase 1: fetching publications per store…\n")

    all_pubs: dict[str, dict] = {}
    new_entries = 0

    for store_code, store_info in stores.items():
        name = store_info.get("name", store_code)
        pubs = fetch_store_publications(brand, store_code)

        existing_ids = {str(p.get("id", "")) for p in store_flyers.get(store_code, [])}
        new_pubs     = [p for p in pubs if str(p.get("id", "")) not in existing_ids]

        if new_pubs:
            store_flyers.setdefault(store_code, []).extend(new_pubs)
            new_entries += len(new_pubs)

        for pub in new_pubs:
            pid = str(pub.get("id", ""))
            if pid and pid not in all_pubs:
                all_pubs[pid] = pub

        log(f"  [{store_code}] {name} → "
            f"{[str(p.get('id','?')) for p in new_pubs] or 'no new flyers'}")
        time.sleep(DELAY)

    log(f"\n  {new_entries} new publication entries across {len(stores)} stores")
    log(f"  {len(all_pubs)} unique new publication(s) to download")
    save_json(store_flyers_path, store_flyers, log)

    summary(f"New publication entries added to store_flyers.json: {new_entries}")
    summary(f"Unique new publications to download: {len(all_pubs)}")

    # ── Phase 2: download new flyer products ──────────────────────────────────
    log("\nPhase 2: fetching products for each new publication…\n")

    os.makedirs(flyers_dir, exist_ok=True)
    downloaded = skipped = errors = 0

    if not all_pubs:
        log("  Nothing new to download.")
    else:
        for pub_id, pub_meta in all_pubs.items():
            out_path = os.path.join(flyers_dir, f"{pub_id}.json")

            if os.path.exists(out_path):
                log(f"  [{pub_id}] already downloaded — skipping")
                skipped += 1
                continue

            name       = pub_meta.get("name", pub_id)
            valid_from = pub_meta.get("valid_from", pub_meta.get("start_date", "?"))
            valid_to   = pub_meta.get("valid_to",   pub_meta.get("end_date",   "?"))
            products_url = (
                f"https://dam.flippenterprise.net/flyerkit/publication/{pub_id}/products"
                f"?display_type=all&locale=en&access_token={brand.access_token}"
            )

            log(f"  [{pub_id}] {name}  ({valid_from} → {valid_to})")

            products = fetch_publication_products(pub_id, brand.access_token)
            if not products:
                log(f"    [!] no products returned — skipping")
                errors += 1
                continue

            log(f"    {len(products)} products")
            save_json(out_path, {
                "fetched_on":       today,
                "publication_id":   pub_id,
                "publication_meta": pub_meta,
                "products_url":     products_url,
                "products":         products,
            }, log)
            downloaded += 1
            time.sleep(DELAY)

    run_end    = now_utc()
    elapsed    = run_end - run_start
    elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"

    summary()
    summary("── Phase 2 results ──────────────────────────────────────────────────")
    summary(f"  Downloaded : {downloaded}")
    summary(f"  Skipped    : {skipped}  (file already existed)")
    summary(f"  Errors     : {errors}")
    summary()
    summary(f"Run completed: {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary(f"Elapsed      : {elapsed_str}")
    summary()

    log(f"\nDone.  Verbose log → {logger.verbose_path}")
    log(f"       Summary log → {logger.summary_path}\n")
    logger.close()


def main() -> None:
    today = now_utc().strftime("%Y-%m-%d")

    for brand in LOBLAWS_PORTFOLIO:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        stores_path = f"data/{brand.folder}/stores.json"

        if brand.slug is None:
            print(f"  [!] Slug not yet confirmed for {brand.name} — skipping.\n")
            continue

        if not os.path.exists(stores_path):
            print(f"  [!] {stores_path} not found — run the store scanner first.\n")
            continue

        fetch_brand_flyers(brand, today)

    print("All brands processed.")


if __name__ == "__main__":
    main()
