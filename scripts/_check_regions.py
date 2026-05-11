"""Deep-dive into flyer_run_id groupings (Flipp) and zonedPublicationName (Metro)."""
import pyarrow.parquet as pq, json, os
from collections import defaultdict

# ── Flipp: check flyer_run_id groupings ───────────────────────────────────────
print("=== Flipp: flyer_run_id groupings ===")
for folder in ["loblaws", "nofrills", "walmart", "sobeys"]:
    path = f"/app/data/{folder}/store_flyers.parquet"
    if not os.path.exists(path): continue
    t = pq.read_table(path)
    rows = t.to_pydict()
    codes = rows["store_code"]
    fids = rows["flyer_id"]
    raws = rows["raw_json"]

    # Build run_id -> list of (store_code, valid_from, valid_to)
    run_groups = defaultdict(list)
    for code, raw in zip(codes, raws):
        obj = json.loads(raw) if raw else {}
        run_id = obj.get("flyer_run_id")
        vf = obj.get("valid_from", "")[:10]
        vt = obj.get("valid_to", "")[:10]
        if run_id:
            run_groups[run_id].append((code, vf, vt))

    # Find runs with multiple stores active at same time
    multi_runs = {rid: stores for rid, stores in run_groups.items() if len(stores) > 1}
    # Show a sample run
    sample_run = max(multi_runs.items(), key=lambda x: len(x[1])) if multi_runs else None
    print(f"\n{folder}:")
    print(f"  total rows: {len(codes)}")
    print(f"  unique flyer_run_ids: {len(run_groups)}")
    print(f"  multi-store runs: {len(multi_runs)}")
    if sample_run:
        rid, stores = sample_run
        print(f"  largest run (id={rid}): {len(stores)} stores, date={stores[0][1]}..{stores[0][2]}")
        print(f"  sample store_codes: {[s[0] for s in stores[:8]]}")

# ── Metro: check zonedPublicationName and title groupings ─────────────────────
print("\n=== Metro: zonedPublicationName and title groupings ===")
for folder in ["metro", "food_basics", "super_c"]:
    path = f"/app/data/{folder}/store_flyers.parquet"
    if not os.path.exists(path): continue
    t = pq.read_table(path)
    rows = t.to_pydict()
    codes = rows["store_code"]
    fids = rows["flyer_id"]  # = job title e.g. "83124"
    raws = rows["raw_json"]

    title_groups = defaultdict(list)
    zoned_groups = defaultdict(list)
    for code, fid, raw in zip(codes, fids, raws):
        obj = json.loads(raw) if raw else {}
        title = obj.get("title")
        zoned = obj.get("zonedPublicationName", "")
        start = obj.get("startDate", "")[:10]
        end   = obj.get("endDate", "")[:10]
        if title:
            title_groups[title].append((code, start, end, zoned))
        if zoned:
            zoned_groups[zoned].append((code, start, end))

    sample_title = max(title_groups.items(), key=lambda x: len(x[1])) if title_groups else None
    print(f"\n{folder}:")
    print(f"  unique job titles (flyer regions): {len(title_groups)}")
    print(f"  unique zonedPublicationNames: {len(zoned_groups)}")
    # Show all unique titles and their store counts
    for tid, stores in sorted(title_groups.items(), key=lambda x: -len(x[1]))[:8]:
        zoned = stores[0][3] if stores else ""
        print(f"  title={tid}  stores={len(stores)}  date={stores[0][1]}  zoned={zoned[:50]!r}")
