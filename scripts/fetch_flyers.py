"""
Fetch weekly flyers for all portfolio brands.

Portfolios:
  loblaws   — Flipp API (Loblaws, No Frills, Provigo, …)
  sobeys    — Flipp API (Sobeys, Safeway, IGA, …)
  walmart   — Flipp API (Walmart Canada)
  metro     — Azure API (Metro, Food Basics, Adonis, Super C, Metro QC)

For each brand:
  Phase 1 — fetch active publications/flyers per store, merge into
                         data/<folder>/store_flyers.parquet  (append-only, no duplicates)
  Phase 2 — download products for each new publication/flyer and append to
             data/<folder>/flyers.parquet      (skip if flyer_id already present)

Logs: logs/<folder>/<date>_verbose.log  /  logs/<folder>/<date>_summary.log

Safe to re-run: existing data is never overwritten.

Usage:
  python -m scripts.fetch_flyers                                # all portfolios
  python -m scripts.fetch_flyers --portfolio loblaws
  python -m scripts.fetch_flyers --portfolio metro --brand food_basics
"""

import argparse
import json
import os
import time
from datetime import date
from typing import Any

from fetchers.azure import (
    METRO_PORTFOLIO,
    MetroBrand,
    metro_fetch_products,
    metro_fetch_store_flyers,
    metro_load_credentials,
)
from fetchers.flipp import (
    DELAY,
    LOBLAWS_PORTFOLIO,
    SOBEYS_PORTFOLIO,
    WALMART_PORTFOLIO,
    Brand,
    FlippLogger,
    fetch_publication_products,
    fetch_store_publications,
    now_utc,
)


# ── Parquet helpers ───────────────────────────────────────────────────────────


def _load_known_flyer_ids(parquet_path: str) -> set[str]:
    """Return the set of ``flyer_id`` values already present in *parquet_path*.

    Returns an empty set when the file does not exist or cannot be read.
    """
    if not os.path.exists(parquet_path):
        return set()
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(parquet_path, columns=["flyer_id"])
        return {str(v) for v in table.column("flyer_id").to_pylist() if v is not None}
    except Exception:
        return set()


def _flatten_flipp_products(
    pub_id: str,
    fetched_on: str,
    pub_meta: dict,
    products_url: str,
    products: list[dict],
) -> list[dict]:
    """Return one row per product for a Flipp publication."""
    envelope: dict[str, Any] = {
        "flyer_id": pub_id,
        "source_api": "flipp",
        "fetched_on": fetched_on,
        "pub_valid_from": pub_meta.get("valid_from"),
        "pub_valid_to": pub_meta.get("valid_to"),
        "pub_locale": pub_meta.get("locale"),
        "products_url": products_url,
    }
    rows: list[dict] = []
    for product in products:
        row = dict(envelope)
        for key, val in product.items():
            row[key] = json.dumps(val) if isinstance(val, (list, dict)) else val
        rows.append(row)
    return rows


def _flatten_metro_products(
    job: str,
    store_id: int | None,
    fetched_on: str,
    products_url: str,
    products: list[dict],
) -> list[dict]:
    """Return one row per product for a Metro job."""
    envelope: dict[str, Any] = {
        "flyer_id": job,
        "source_api": "metro",
        "fetched_on": fetched_on,
        "store_id": str(store_id) if store_id is not None else None,
        "products_url": products_url,
    }
    rows: list[dict] = []
    for product in products:
        row = dict(envelope)
        for key, val in product.items():
            row[key] = json.dumps(val) if isinstance(val, (list, dict)) else val
        rows.append(row)
    return rows


def _load_stores_parquet(parquet_path: str) -> dict[str, dict]:
    """Return ``{store_code: {province, store_name, ...}}`` from stores.parquet."""
    if not os.path.exists(parquet_path):
        return {}
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(parquet_path)
        result: dict[str, dict] = {}
        for row in table.to_pylist():
            code = str(row.get("store_code", ""))
            if code:
                result[code] = json.loads(row["raw_json"]) if "raw_json" in row else row
        return result
    except Exception:
        return {}


def _load_store_flyers_parquet(parquet_path: str) -> dict[str, list]:
    """Return ``{store_code: [pub_dict, ...]}`` from store_flyers.parquet."""
    if not os.path.exists(parquet_path):
        return {}
    try:
        import pyarrow.parquet as pq
        table = pq.read_table(parquet_path)
        result: dict[str, list] = {}
        for row in table.to_pylist():
            code = str(row.get("store_code", ""))
            pub = json.loads(row["raw_json"]) if "raw_json" in row else {"id": row.get("flyer_id")}
            result.setdefault(code, []).append(pub)
        return result
    except Exception:
        return {}


def _append_store_flyers_parquet(parquet_path: str, new_rows: list[dict]) -> None:
    """Append new store-flyer rows to store_flyers.parquet."""
    if not new_rows:
        return
    import pyarrow as pa
    import pyarrow.parquet as pq

    new_table = pa.Table.from_pylist(new_rows)
    if os.path.exists(parquet_path):
        existing = pq.read_table(parquet_path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        os.makedirs(os.path.dirname(parquet_path) or ".", exist_ok=True)
        combined = new_table
    pq.write_table(combined, parquet_path)


def _append_raw_parquet(parquet_path: str, new_rows: list[dict]) -> None:
    """Append *new_rows* to the brand Parquet file, creating it if absent."""
    if not new_rows:
        return
    import pyarrow as pa
    import pyarrow.parquet as pq

    new_table = pa.Table.from_pylist(new_rows)
    if os.path.exists(parquet_path):
        existing = pq.read_table(parquet_path)
        combined = pa.concat_tables([existing, new_table], promote_options="default")
    else:
        combined = new_table
    os.makedirs(os.path.dirname(parquet_path) or ".", exist_ok=True)
    pq.write_table(combined, parquet_path)


# ── Flipp brand flyer fetcher ─────────────────────────────────────────────────

def fetch_flipp_brand(brand: Brand, today: str) -> None:
    stores_parquet_path       = f"data/{brand.folder}/stores.parquet"
    store_flyers_parquet_path = f"data/{brand.folder}/store_flyers.parquet"
    parquet_path              = f"data/{brand.folder}/flyers.parquet"
    log_dir                   = f"logs/{brand.folder}"

    logger  = FlippLogger(log_dir, today)
    log     = logger.log
    summary = logger.summary

    run_start = now_utc()
    summary(f"=== {brand.name} Flyer Fetch — {today} ===")
    summary(f"Run started : {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary()

    # ── Load stores ───────────────────────────────────────────────────────────
    stores = _load_stores_parquet(stores_parquet_path)

    log(f"Loaded {len(stores)} stores from {stores_parquet_path}\n")
    summary(f"Stores loaded: {len(stores)}")

    # ── Load existing store_flyers accumulator ────────────────────────────────
    store_flyers: dict[str, list] = _load_store_flyers_parquet(store_flyers_parquet_path)
    log(f"Loaded store_flyers.parquet "
        f"({sum(len(v) for v in store_flyers.values())} publication entries)\n")

    # ── Phase 1: store → publications ─────────────────────────────────────────
    log("Phase 1: fetching publications per store…\n")

    all_pubs: dict[str, dict] = {}
    new_entries = 0
    new_store_flyer_rows: list[dict] = []

    for store_code, store_info in stores.items():
        name = store_info.get("name", store_code)
        pubs = fetch_store_publications(brand, store_code)

        existing_ids = {str(p.get("id", "")) for p in store_flyers.get(store_code, [])}
        new_pubs     = [p for p in pubs if str(p.get("id", "")) not in existing_ids]

        if new_pubs:
            store_flyers.setdefault(store_code, []).extend(new_pubs)
            new_entries += len(new_pubs)
            for pub in new_pubs:
                new_store_flyer_rows.append({
                    "store_code": store_code,
                    "flyer_id": str(pub.get("id", "")),
                    "raw_json": json.dumps(pub),
                })

        for pub in new_pubs:
            pid = str(pub.get("id", ""))
            if pid and pid not in all_pubs:
                all_pubs[pid] = pub

        log(f"  [{store_code}] {name} → "
            f"{[str(p.get('id','?')) for p in new_pubs] or 'no new flyers'}")
        time.sleep(DELAY)

    log(f"\n  {new_entries} new publication entries across {len(stores)} stores")
    log(f"  {len(all_pubs)} unique new publication(s) to download")
    _append_store_flyers_parquet(store_flyers_parquet_path, new_store_flyer_rows)
    log(f"  Appended {len(new_store_flyer_rows)} rows to {store_flyers_parquet_path}")

    summary(f"New publication entries added to store_flyers.parquet: {new_entries}")
    summary(f"Unique new publications to download: {len(all_pubs)}")

    # ── Phase 2: download new flyer products ──────────────────────────────────
    log("\nPhase 2: fetching products for each new publication…\n")

    known_ids = _load_known_flyer_ids(parquet_path)
    downloaded = skipped = errors = 0
    new_rows: list[dict] = []

    if not all_pubs:
        log("  Nothing new to download.")
    else:
        for pub_id, pub_meta in all_pubs.items():
            if pub_id in known_ids:
                log(f"  [{pub_id}] already in Parquet — skipping")
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
                log("    [!] no products returned — skipping")
                errors += 1
                continue

            log(f"    {len(products)} products")
            new_rows.extend(
                _flatten_flipp_products(pub_id, today, pub_meta, products_url, products)
            )
            downloaded += 1
            time.sleep(DELAY)

    _append_raw_parquet(parquet_path, new_rows)
    _log_phase2_summary(summary, log, logger, run_start, downloaded, skipped, errors)


# ── Metro brand flyer fetcher ─────────────────────────────────────────────────

def fetch_metro_brand(brand: MetroBrand, today: str) -> None:
    stores_parquet_path       = f"data/{brand.folder}/stores.parquet"
    store_flyers_parquet_path = f"data/{brand.folder}/store_flyers.parquet"
    parquet_path              = f"data/{brand.folder}/flyers.parquet"
    log_dir                   = f"logs/{brand.folder}"

    logger  = FlippLogger(log_dir, today)
    log     = logger.log
    summary = logger.summary

    run_start = now_utc()
    summary(f"=== {brand.name} Flyer Fetch — {today} ===")
    summary(f"Run started : {run_start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    summary()

    # ── Load stores ───────────────────────────────────────────────────────────
    stores = _load_stores_parquet(stores_parquet_path)

    log(f"Loaded {len(stores)} stores from {stores_parquet_path}\n")
    summary(f"Stores loaded: {len(stores)}")

    # ── Load existing store_flyers accumulator ────────────────────────────────
    store_flyers: dict[str, list] = _load_store_flyers_parquet(store_flyers_parquet_path)
    log(f"Loaded store_flyers.parquet "
        f"({sum(len(v) for v in store_flyers.values())} flyer entries)\n")

    # ── Phase 1: store → flyers ───────────────────────────────────────────────
    log("Phase 1: fetching flyer listings per store…\n")

    # new_jobs: job_number -> store_id (representative store used for download)
    new_jobs: dict[str, int] = {}
    new_entries = 0
    new_store_flyer_rows: list[dict] = []

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
                new_store_flyer_rows.append({
                    "store_code": store_id_str,
                    "flyer_id": flyer.get("title", ""),
                    "raw_json": json.dumps(flyer),
                })

        for flyer in new_flyers:
            job = flyer.get("title", "")
            if job and job not in new_jobs:
                new_jobs[job] = store_id

        log(f"  [{store_id}] {name} → "
            f"{[f.get('title', '?') for f in new_flyers] or 'no new flyers'}")
        time.sleep(DELAY)

    log(f"\n  {new_entries} new flyer entries across {len(stores)} stores")
    log(f"  {len(new_jobs)} unique new job(s) to download")
    _append_store_flyers_parquet(store_flyers_parquet_path, new_store_flyer_rows)
    log(f"  Appended {len(new_store_flyer_rows)} rows to {store_flyers_parquet_path}")

    summary(f"New flyer entries added to store_flyers.parquet : {new_entries}")
    summary(f"Unique new jobs to download                     : {len(new_jobs)}")

    # ── Phase 2: download new flyer products ──────────────────────────────────
    log("\nPhase 2: fetching products for each new job…\n")

    known_ids = _load_known_flyer_ids(parquet_path)
    downloaded = skipped = errors = 0
    new_rows: list[dict] = []

    if not new_jobs:
        log("  Nothing new to download.")
    else:
        for job, store_id in new_jobs.items():
            if job in known_ids:
                log(f"  [{job}] already in Parquet — skipping")
                skipped += 1
                continue

            products_url = (
                f"https://metrodigital-apim.azure-api.net/api"
                f"/Pages/{job}/{store_id}/{brand.locale}/search"
            )
            log(f"  [{job}] store {store_id}  ({products_url})")

            products = metro_fetch_products(brand, job, store_id)
            if not products:
                log("    [!] no products returned — skipping")
                errors += 1
                continue

            log(f"    {len(products)} products")
            new_rows.extend(
                _flatten_metro_products(job, store_id, today, products_url, products)
            )
            downloaded += 1
            time.sleep(DELAY)

    _append_raw_parquet(parquet_path, new_rows)
    _log_phase2_summary(summary, log, logger, run_start, downloaded, skipped, errors)


# ── Shared summary helper ─────────────────────────────────────────────────────

def _log_phase2_summary(summary, log, logger, run_start, downloaded, skipped, errors) -> None:
    run_end     = now_utc()
    elapsed     = run_end - run_start
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


# ── Portfolio runners ─────────────────────────────────────────────────────────

def run_flipp_portfolio(brands: list[Brand], today: str) -> None:
    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if brand.slug is None:
            print(f"  [!] Slug not yet confirmed for {brand.name} — skipping.\n")
            continue

        stores_path = f"data/{brand.folder}/stores.parquet"
        if not os.path.exists(stores_path):
            print(f"  [!] {stores_path} not found — run fetch_stores.py first.\n")
            continue

        fetch_flipp_brand(brand, today)


def run_metro_portfolio(brands: list[MetroBrand], today: str) -> None:
    for brand in brands:
        print(f"{'─' * 60}")
        print(f"Brand: {brand.name}  (folder: {brand.folder})")

        if not metro_load_credentials(brand):
            print(f"  [!] Could not load credentials for {brand.name} — skipping.\n")
            continue

        stores_path = f"data/{brand.folder}/stores.parquet"
        if not os.path.exists(stores_path):
            print(f"  [!] {stores_path} not found — run fetch_stores.py first.\n")
            continue

        fetch_metro_brand(brand, today)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _filter_brands(portfolio: list, brand_arg: str | None, portfolio_name: str) -> list:
    if brand_arg is None:
        return portfolio
    filtered = [b for b in portfolio if b.folder == brand_arg]
    if not filtered:
        valid = [b.folder for b in portfolio]
        raise SystemExit(
            f"Unknown brand folder '{brand_arg}' for portfolio '{portfolio_name}'. "
            f"Valid: {valid}"
        )
    return filtered


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch weekly flyers for grocery portfolio brands"
    )
    parser.add_argument(
        "--portfolio",
        choices=["loblaws", "sobeys", "walmart", "metro"],
        help="Portfolio to fetch. Omit to fetch all.",
    )
    parser.add_argument(
        "--brand",
        metavar="FOLDER",
        help="Single brand folder to fetch (e.g. nofrills, food_basics).",
    )
    args = parser.parse_args()

    today = date.today().isoformat()
    portfolios = [args.portfolio] if args.portfolio else ["loblaws", "sobeys", "walmart", "metro"]

    for portfolio in portfolios:
        if portfolio == "loblaws":
            brands = _filter_brands(LOBLAWS_PORTFOLIO, args.brand, "loblaws")
            run_flipp_portfolio(brands, today)
        elif portfolio == "sobeys":
            brands = _filter_brands(SOBEYS_PORTFOLIO, args.brand, "sobeys")
            run_flipp_portfolio(brands, today)
        elif portfolio == "walmart":
            brands = _filter_brands(WALMART_PORTFOLIO, args.brand, "walmart")
            run_flipp_portfolio(brands, today)
        elif portfolio == "metro":
            brands = _filter_brands(METRO_PORTFOLIO, args.brand, "metro")
            run_metro_portfolio(brands, today)

    print("All brands processed.")


if __name__ == "__main__":
    main()
