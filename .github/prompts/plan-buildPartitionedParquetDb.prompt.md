## Plan: Build Partitioned Parquet + DuckDB Database

The goal is to transform the current per-flyer JSON files into a queryable, append-only analytical store. Since `pyarrow` is already in `requirements.txt` and `clean.py` already has Parquet writing helpers, the lift is small.

---

**TL;DR** — Create `pipeline/build_db.py`: it reads `cleaned/**/*.json` and writes one Parquet file per flyer into a Hive-partitioned `db/` directory. DuckDB queries the whole thing in-process via glob. Weekly runs only write new flyer files (dedup by file existence) so re-running is always safe.

---

### `db/` layout

```
db/
  observations/
    store_chain=loblaws/year=2026/week=14/
      83006.parquet          ← one file per flyer_id, ~500 rows
      83007.parquet
    store_chain=metro/year=2026/week=14/
      83001.parquet
  dimensions/
    stores.parquet           ← one row per known store, rebuilt each run
    flyers.parquet           ← one row per flyer, rebuilt each run
```

One Parquet file per `flyer_id` is the key design choice — it eliminates any "read-merge-write" complexity. Checking whether a flyer is already ingested is just `os.path.exists()`.

---

### Phase 1 — Add dependency

**Step 1.** Add `duckdb` to `requirements.txt`. `pyarrow` is already there.

---

### Phase 2 — Create `pipeline/build_db.py`

**Step 2a.** Implement `_partition_dir(db_dir, store_chain, flyer_valid_from)` — a pure helper that returns the partition path like `db/observations/store_chain=loblaws/year=2026/week=14`. Derive `year`/`week` from `flyer_valid_from`; fall back to `fetched_on` if null; fall back to the current date as last resort.

**Step 2b.** Implement `build_observations(db_dir, cleaned_dir, store=None, force=False)`:
- Iterates all `cleaned/<store>/<flyer_id>.json` envelopes (or just one brand if `--store` given)
- For each flyer, checks if `<partition_dir>/<flyer_id>.parquet` already exists → skips if so (unless `--force`)
- Loads `records[]` from the JSON, serializes list fields to JSON strings (same pattern as the existing `_write_parquet` in `pipeline/clean.py`)
- Builds a `pyarrow.Table` and writes with `pq.write_table()`
- Prints a summary line per brand

**Step 2c.** Implement `build_dimensions(db_dir, data_dir)`:
- **`stores.parquet`** — walks `data/*/stores.json`, flattens to one row per store_id with columns: `store_chain`, `store_id`, `store_name`, `banner`, `province`, `city`, `postal_code`
- **`flyers.parquet`** — walks `data/*/store_flyers.json`, flattens to one row per unique flyer with columns: `flyer_id`, `store_chain`, `store_id`, `valid_from`, `valid_to`, `language`, `province`, `fetched_on`
- Both files are fully rebuilt each run (they're small/fast)

**Step 2d.** Implement `main()` with `argparse`:
- `--db-dir db` (default)
- `--cleaned-dir cleaned` (default)
- `--data-dir data` (default)
- `--store <name>` — restrict to one brand
- `--force` — re-ingest all even if files exist
- `--dimensions-only` — rebuild dim tables only (no observations touched)
- Invocable as `python -m pipeline.build_db`

---

### Phase 3 — Weekly integration

**Step 3.** The canonical three-step weekly workflow becomes:

```
python scripts/fetch_flyers.py          # 1. scrape → data/
python -m pipeline.clean                # 2. normalize → cleaned/
python -m pipeline.build_db             # 3. ingest → db/
```

Steps 2 and 3 are independent and idempotent — running them in any order or re-running them is safe. Step 3 skips any `flyer_id` whose Parquet file already exists.

---

### Phase 4 — Query layer (no code needed)

DuckDB reads the whole dataset in-process with no server, no import:

```python
import duckdb
con = duckdb.connect()
con.sql("""
    SELECT store_chain, name_en, sale_price, flyer_valid_from
    FROM read_parquet('db/observations/**/*.parquet', hive_partitioning=true)
    WHERE name_en ILIKE '%chicken breast%'
      AND year = 2026
    ORDER BY sale_price
""").show()
```

The `hive_partitioning=true` flag makes DuckDB use `store_chain`/`year`/`week` from the folder names as filter push-down predicates, so brand-scoped or week-scoped queries skip irrelevant files entirely.

---

### Relevant files

- `pipeline/clean.py` — reuse `_write_parquet()` pattern and the list-field JSON-serialization trick
- `pipeline/load_raw.py` — `iter_flyers()` used as reference; `build_db.py` reads `cleaned/` directly instead
- `pipeline/schema.py` — `FlyerItem` field list drives the Parquet column set
- `requirements.txt` — add `duckdb`
- `pipeline/build_db.py` — new file (sole deliverable)

---

### Verification

1. Run `python -m pipeline.build_db` — confirm `db/observations/` and `db/dimensions/` are created
2. Run it again — confirm zero new files are written (idempotency)
3. Run `python -m pipeline.build_db --force` — confirm all files are rewritten
4. Open a Python REPL and run `duckdb.connect().sql("SELECT COUNT(*) FROM read_parquet('db/observations/**/*.parquet', hive_partitioning=true)")` — should return ~223K
5. Confirm `db/dimensions/stores.parquet` and `flyers.parquet` exist and have correct row counts

---

### Decisions

- **One Parquet per flyer** (not one per partition): simplest dedup, no read-merge-write, DuckDB handles the multi-file scan transparently
- **Dimensions are rebuilt, not appended**: `stores.json` is append-only already; a full rebuild of the small dim tables is simpler than diffing them
- **`cleaned/all_flyers.parquet`** from `clean.py` is left as-is for now — it serves as a quick single-file dump; `db/` is the queryable store
- **`db/` in `.gitignore`**: Parquet files are build artifacts derived from `cleaned/`; recommend tracking `cleaned/` (source of truth) but not `db/`

---

### Scaling path (decisions baked in now)

| Stage | Action |
|---|---|
| **Now (< 5M rows)** | `python -m pipeline.build_db` + DuckDB in-process |
| **Medium (5–50M)** | `ATTACH 'md:grocery_flyer'` to MotherDuck — same SQL, same Parquet files |
| **Large (50M+)** | `bq load db/observations/**/*.parquet` to BigQuery — Hive partitions map directly to BQ partition columns |
