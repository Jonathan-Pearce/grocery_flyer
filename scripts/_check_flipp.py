"""Check Flipp store raw_json fields."""
import pyarrow.parquet as pq, json
t = pq.read_table("/app/data/loblaws/stores.parquet")
rows = t.to_pydict()
print("columns:", t.column_names)
raw = rows["raw_json"][0]
print("raw_json keys:", list(json.loads(raw).keys()))
print("sample raw_json:", raw[:500])
