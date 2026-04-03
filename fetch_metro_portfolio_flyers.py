"""
Weekly flyer fetcher for all Metro portfolio brands.

For each brand with credentials and a data/<folder>/stores.json:
  Phase 1 — fetch active flyer listings per store, merge into
             data/<folder>/store_flyers.json  (append-only, no duplicates)
             Deduplication key: flyer job number (flyers[].title)
  Phase 2 — download products for each new unique job into
             data/<folder>/flyers/<job>.json  (skip if file already exists)
             Metro flyer products require store_id in the URL; one store per
             job is used as representative (all stores sharing a job get the
             same product set).

Logs:
  logs/<folder>/<date>_verbose.log
  logs/<folder>/<date>_summary.log

Safe to re-run: existing data is never overwritten.
"""

import argparse
import json
import os
import time

from datetime import date

from flipp import (
    DELAY,
    METRO_PORTFOLIO,
    MetroBrand,
    FlippLogger,
    metro_fetch_store_flyers,
    metro_fetch_products,
    metro_load_credentials,
    now_utc,
    save_json,
)


def fetch_brand_flyers(brand: MetroBrand, today: str) -> None:
    stores_path       = f"data/{brand.folder}/stores.json"
    store_flyers_path = f"data/{brand.folder}/store_flyers.json"
    flyers_dir        = f"data/{brand.folder}/flyers"
    log_dir           = f"logs/{brand.folder}"

    logger  = FlippLogger(log_dir, today)
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
        log(
            f"Loaded existing store_flyers.json "
            f"({sum(len(v) for v in store_flyers.values())} flyer entries)\n"
        )
    else:
        store_flyers = {}

    # ── Phase 1: store → flyers ───────────────────────────────────────────────
    log("Phase 1: fetching flyer listings per store…\n")

    # new_jobs: job_number -> store_id (representative store used for download)
    new_jobs: dict[str, int] = {}
    new_entries = 0

    for store_id_str, store_info in stores.items():
        store_id  = int(store_id_str)
        name      = store_info.get("store_name", store_id_str)
        flyers    = metro_fetch_store_flyers(brand, store_id, today)

        existing_jobs = {f.get("title", "") for f in store_flyers.get(store_id_str, [])}
        new_flyers    = [f for f in flyers if f.get("title", "") not in existing_jobs]

        if new_flyers:
            store_flyers.setdefault(store_id_str, []).extend(new_flyers)
            new_entries += len(new_flyers)

        for flyer in new_flyers:
            job = flyer.get("title", "")
            if job and job not in new_jobs:
                new_jobs[job] = store_id

        log(
            f"  [{store_id}] {name} → "
            f"{[f.get('title', '?') for f in new_flyers] or 'no new flyers'}"
        )
        time.sleep(DELAY)

    log(f"\n  {new_entries} new flyer entries across {len(stores)} stores")
    log(f"  {len(new_jobs)} unique new job(s) to download")
    save_json(store_flyers_path, store_flyers, log)

    summary(f"New flyer entries added to store_flyers.json : {new_entries}")
    summary(f"Unique new jobs to download                  : {len(new_jobs)}")

    # ── Phase 2: download new flyer products ──────────────────────────────────
    log("\nPhase 2: fetching products for each new job…\n")

    os.makedirs(flyers_dir, exist_ok=True)
    downloaded = skipped = errors = 0

    if not new_jobs:
        log("  Nothing new to download.")
    else:
        for job, store_id in new_jobs.items():
            out_path = os.path.join(flyers_dir, f"{job}.json")

            if os.path.exists(out_path):
                log(f"  [{job}] already downloaded — skipping")
                skipped += 1
                continue

            products_url = (
                f"https://metrodigital-apim.azure-api.net/api"
                f"/Pages/{job}/{store_id}/en/search"
            )
            log(f"  [{job}] store {store_id}  ({products_url})")

            products = metro_fetch_products(brand, job, store_id)
            if not products:
                log(f"    [!] no products returned — skipping")
                errors += 1
                continue

            log(f"    {len(products)} products")
            save_json(
                out_path,
                {
                    "fetched_on":  today,
                    "job":         job,
                    "store_id":    store_id,
                    "products_url": products_url,
                    "products":    products,
                },
                log,
            )
            downloaded += 1
            time.sleep(DELAY)

    run_end     = now_utc()
    elapsed     = run_end - run_start
    elapsed_str = f"{int(elapsed.total_seconds() // 60)}m {int(elapsed.total_seconds() % 60)}s"

    summary()
    summary("── Phase 2 results ──────────────────────────────────────────────────")
    summary(f"  Downloaded : {downloaded}")
    summary(f"  Skipped    : {skipped}  (file already existed)")
    summary(f"  Errors     : {errors}")
    summary()
    summary(f"Run completed : {run_end.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary(f"Elapsed       : {elapsed_str}")
    summary()

    log(f"\nDone.  Verbose log → {logger.verbose_path}")
    log(f"       Summary log → {logger.summary_path}\n")
    logger.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Metro portfolio flyers")
    parser.add_argument(
        "--brand",
        metavar="FOLDER",
        help="Folder name of a single brand to process (e.g. metro, food_basics). "
             "Omit to run all brands.",
    )
    args = parser.parse_args()

    today = date.today().isoformat()

    brands = METRO_PORTFOLIO
    if args.brand:
        brands = [b for b in brands if b.folder == args.brand]
        if not brands:
            parser.error(
                f"Unknown brand folder '{args.brand}'. "
                f"Valid: {[b.folder for b in METRO_PORTFOLIO]}"
            )

    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if not metro_load_credentials(brand):
            print(f"  [!] Could not load credentials for {brand.name} — skipping.\n")
            continue

        stores_path = f"data/{brand.folder}/stores.json"
        if not os.path.exists(stores_path):
            print(
                f"  [!] {stores_path} not found — "
                f"run fetch_metro_portfolio_stores.py first.\n"
            )
            continue

        fetch_brand_flyers(brand, today)

    print(f"{'─' * 60}")
    print("All brands processed.")


if __name__ == "__main__":
    main()
