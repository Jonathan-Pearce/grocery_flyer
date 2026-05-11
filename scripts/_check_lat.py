"""Check why Flipp stores don't have lat/lon."""
import pyarrow.parquet as pq, json

for folder in ["loblaws", "nofrills", "walmart", "sobeys"]:
    t = pq.read_table(f"/app/data/{folder}/stores.parquet")
    rows = t.to_pydict()
    raws = rows["raw_json"]
    has_lat = 0
    null_lat = 0
    sample_null = None
    for raw in raws:
        obj = json.loads(raw) if raw else {}
        lat = obj.get("latitude")
        if lat is not None:
            has_lat += 1
        else:
            null_lat += 1
            if sample_null is None:
                sample_null = {k: v for k, v in obj.items() if k in ("name","latitude","longitude","postal_code","city")}
    print(f"{folder}: has_lat={has_lat}  null_lat={null_lat}")
    if sample_null:
        print(f"  null sample: {sample_null}")
