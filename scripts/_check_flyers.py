"""Check store_flyers.parquet structure across chains."""
import json
import os

import pyarrow.parquet as pq

for folder in ["loblaws", "nofrills", "metro", "food_basics", "super_c", "walmart"]:
    path = f"/app/data/{folder}/store_flyers.parquet"
    if not os.path.exists(path):
        print(f"{folder}: NO FILE")
        continue
    t = pq.read_table(path)
    rows = t.to_pydict()
    print(f"\n{folder}: {t.num_rows} rows, columns={t.column_names}")
    raws = rows.get("raw_json", [])
    if raws:
        obj = json.loads(raws[0]) if raws[0] else {}
        print(f"  raw_json keys: {list(obj.keys())}")
        print(f"  sample: {raws[0][:200]}")
    # How many unique flyer_ids?
    fids = rows.get("flyer_id", [])
    print(f"  unique flyer_ids: {len(set(fids))}")
    # How many unique store_codes?
    scodes = rows.get("store_code", [])
    print(f"  unique store_codes: {len(set(scodes))}")
    # Check for stores with multiple flyers in a week
    from collections import Counter
    code_counts = Counter(scodes)
    multi = {c: v for c, v in code_counts.items() if v > 1}
    print(f"  stores with >1 flyer: {len(multi)} (max={max(code_counts.values()) if code_counts else 0})")
