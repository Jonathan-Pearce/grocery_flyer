"""Sanity check on exported JSON files."""
import json

with open("/app/frontend/public/data/stores_geo.json") as f:
    stores = json.load(f)

print(f"stores_geo.json: {len(stores)} records")
print("Sample Flipp store:", stores[0])
metro = next((s for s in stores if s["chain"] == "metro" and s["lat"]), None)
print("Sample Metro store (with geo):", metro)
fb = next((s for s in stores if s["chain"] == "food_basics" and s["postal_code"]), None)
print("Sample Food Basics:", fb)

with open("/app/frontend/public/data/flyer_regions.json") as f:
    regions = json.load(f)

print(f"\nflyer_regions.json: {len(regions)} records")
nf = next((r for r in regions if r["chain"] == "nofrills" and r["store_count"] > 100), None)
if nf:
    print("Sample NoFrills region:", {k: v for k, v in nf.items() if k not in ("store_codes","postal_codes")})
    print("  store_codes sample:", nf["store_codes"][:5])
    print("  postal_fsas sample:", nf["postal_fsas"][:10])

multi_samples = [r for r in regions if r["multi_flyer_stores"]][:2]
for r in multi_samples:
    print(f"\nMulti-flyer: chain={r['chain']} region={r['region_id']} week={r['valid_from']}")
    print(f"  multi_stores={r['multi_flyer_stores'][:3]}")
