#!/usr/bin/env python3
"""Generate naive mock deal data for frontend development."""
import json
from pathlib import Path

REGIONS = {
    "loblaws":                 "1152122",
    "nofrills":                "1152332",
    "real_canadian_superstore":"1152174",
    "zehrs":                   "1152277",
    "metro":                   "83124",
    "food_basics":             "82596",
    "sobeys":                  "1068174",
    "safeway":                 "1155870",
    "freshco":                 "1145128",
    "farm_boy":                "1142688",
    "walmart":                 "1178526",
    "maxi":                    "1172082",
    "super_c":                 "83001",
    "iga":                     "1189314",
    "adonis":                  "83006",
}

VALID_FROM = "2026-05-08"
VALID_TO   = "2026-05-14"

DEALS = [
    # Produce
    {"name_en":"Strawberries 1 lb","brand":"","sale_price":2.97,"regular_price":4.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Berries","deal_score":88,"chain":"loblaws"},
    {"name_en":"Bananas","brand":"","sale_price":1.47,"regular_price":1.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Tropical Fruit","deal_score":62,"chain":"nofrills"},
    {"name_en":"Roma Tomatoes","brand":"","sale_price":1.97,"regular_price":3.49,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Vegetables","deal_score":79,"chain":"metro"},
    {"name_en":"Seedless Grapes","brand":"","sale_price":3.97,"regular_price":6.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Fruit","deal_score":72,"chain":"sobeys"},
    {"name_en":"Blueberries 1 pint","brand":"","sale_price":2.49,"regular_price":4.49,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Produce","category_l2":"Berries","deal_score":83,"chain":"real_canadian_superstore"},
    {"name_en":"Broccoli","brand":"","sale_price":1.49,"regular_price":2.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Vegetables","deal_score":71,"chain":"food_basics"},
    {"name_en":"Organic Spinach 142g","brand":"Compliments","sale_price":2.99,"regular_price":4.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Leafy Greens","deal_score":68,"chain":"sobeys"},
    {"name_en":"Asparagus","brand":"","sale_price":2.97,"regular_price":5.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Vegetables","deal_score":77,"chain":"farm_boy"},
    {"name_en":"Mangoes","brand":"","sale_price":0.99,"regular_price":1.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Tropical Fruit","deal_score":82,"chain":"adonis"},
    {"name_en":"Sweet Bell Peppers 3-pack","brand":"","sale_price":3.49,"regular_price":5.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Vegetables","deal_score":70,"chain":"metro"},
    {"name_en":"Baby Carrots 2 lb","brand":"","sale_price":1.99,"regular_price":3.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Vegetables","deal_score":65,"chain":"freshco"},
    {"name_en":"Avocados 4-pack","brand":"","sale_price":4.99,"regular_price":6.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Produce","category_l2":"Fruit","deal_score":68,"chain":"metro"},
    {"name_en":"Watermelon","brand":"","sale_price":4.97,"regular_price":7.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Fruit","deal_score":75,"chain":"maxi"},
    {"name_en":"Romaine Hearts 3-pack","brand":"","sale_price":2.99,"regular_price":4.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Produce","category_l2":"Leafy Greens","deal_score":67,"chain":"super_c"},

    # Meat
    {"name_en":"Chicken Breast Boneless Skinless","brand":"","sale_price":6.97,"regular_price":11.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Chicken","deal_score":92,"chain":"loblaws"},
    {"name_en":"Lean Ground Beef","brand":"","sale_price":7.77,"regular_price":12.49,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Beef","deal_score":87,"chain":"real_canadian_superstore"},
    {"name_en":"Pork Back Ribs","brand":"","sale_price":8.97,"regular_price":14.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Pork","deal_score":80,"chain":"nofrills"},
    {"name_en":"Whole Chicken","brand":"","sale_price":9.97,"regular_price":14.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Chicken","deal_score":75,"chain":"metro"},
    {"name_en":"Extra-Lean Ground Turkey","brand":"Maple Lodge","sale_price":5.99,"regular_price":8.49,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Meat","category_l2":"Poultry","deal_score":74,"chain":"food_basics"},
    {"name_en":"Striploin Grilling Steak","brand":"","sale_price":24.97,"regular_price":36.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Beef","deal_score":78,"chain":"safeway"},
    {"name_en":"Chicken Thighs Bone-In","brand":"","sale_price":5.49,"regular_price":8.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Chicken","deal_score":76,"chain":"walmart"},
    {"name_en":"Baby Back Pork Ribs","brand":"","sale_price":10.97,"regular_price":16.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Meat","category_l2":"Pork","deal_score":82,"chain":"freshco"},

    # Seafood
    {"name_en":"Atlantic Salmon Fillet","brand":"","sale_price":14.97,"regular_price":22.99,"price_unit":"kg","promo_type":"percentage_off","category_l1":"Seafood","category_l2":"Fish","deal_score":85,"chain":"sobeys"},
    {"name_en":"Jumbo Shrimp 31-40 ct","brand":"","sale_price":11.97,"regular_price":19.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Seafood","category_l2":"Shellfish","deal_score":81,"chain":"zehrs"},
    {"name_en":"Smoked Salmon 150g","brand":"Ocean's","sale_price":5.97,"regular_price":9.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Seafood","category_l2":"Fish","deal_score":73,"chain":"farm_boy"},

    # Dairy & Eggs
    {"name_en":"Large Eggs 18-pack","brand":"Burnbrae","sale_price":5.49,"regular_price":8.49,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Dairy & Eggs","category_l2":"Eggs","deal_score":84,"chain":"loblaws"},
    {"name_en":"Milk 4 L","brand":"Natrel","sale_price":4.99,"regular_price":6.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Dairy & Eggs","category_l2":"Milk","deal_score":71,"chain":"metro"},
    {"name_en":"Marble Cheddar 400g","brand":"Black Diamond","sale_price":4.49,"regular_price":6.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Dairy & Eggs","category_l2":"Cheese","deal_score":79,"chain":"nofrills"},
    {"name_en":"Greek Yogurt 750g","brand":"Oikos","sale_price":4.97,"regular_price":7.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Dairy & Eggs","category_l2":"Yogurt","deal_score":74,"chain":"real_canadian_superstore"},
    {"name_en":"Butter 454g","brand":"Gay Lea","sale_price":5.49,"regular_price":7.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Dairy & Eggs","category_l2":"Butter","deal_score":69,"chain":"sobeys"},
    {"name_en":"Brie 200g","brand":"Président","sale_price":3.99,"regular_price":6.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Dairy & Eggs","category_l2":"Cheese","deal_score":82,"chain":"iga"},
    {"name_en":"Cream Cheese 250g","brand":"Philadelphia","sale_price":2.99,"regular_price":4.79,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Dairy & Eggs","category_l2":"Cheese","deal_score":68,"chain":"food_basics"},
    {"name_en":"Sour Cream 500 mL","brand":"Astro","sale_price":2.49,"regular_price":3.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Dairy & Eggs","category_l2":"Cream","deal_score":61,"chain":"zehrs"},

    # Bakery & Bread
    {"name_en":"Whole Wheat Bread 675g","brand":"Dempster's","sale_price":3.49,"regular_price":5.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Bakery & Bread","category_l2":"Bread","deal_score":66,"chain":"loblaws"},
    {"name_en":"Sourdough Boule","brand":"","sale_price":3.99,"regular_price":5.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Bakery & Bread","category_l2":"Artisan Bread","deal_score":72,"chain":"farm_boy"},
    {"name_en":"Croissants 6-pack","brand":"","sale_price":4.99,"regular_price":6.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Bakery & Bread","category_l2":"Pastry","deal_score":69,"chain":"metro"},
    {"name_en":"English Muffins 6-pack","brand":"Thomas'","sale_price":2.99,"regular_price":4.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Bakery & Bread","category_l2":"Muffins","deal_score":63,"chain":"sobeys"},
    {"name_en":"Bagels 6-pack","brand":"","sale_price":3.49,"regular_price":5.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Bakery & Bread","category_l2":"Bagels","deal_score":65,"chain":"maxi"},

    # Pantry & Dry Goods
    {"name_en":"Pasta Penne 900g","brand":"Barilla","sale_price":2.49,"regular_price":3.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Pasta","deal_score":64,"chain":"nofrills"},
    {"name_en":"Olive Oil 750 mL","brand":"Filippo Berio","sale_price":8.97,"regular_price":14.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Oils","deal_score":78,"chain":"loblaws"},
    {"name_en":"Canned Diced Tomatoes 796 mL","brand":"Hunt's","sale_price":1.49,"regular_price":2.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Canned Goods","deal_score":61,"chain":"food_basics"},
    {"name_en":"Brown Rice 2 kg","brand":"Uncle Ben's","sale_price":5.99,"regular_price":9.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Rice & Grains","deal_score":70,"chain":"metro"},
    {"name_en":"All-Purpose Flour 10 kg","brand":"Robin Hood","sale_price":9.97,"regular_price":16.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Baking","deal_score":76,"chain":"real_canadian_superstore"},
    {"name_en":"Chicken Broth 900 mL 2-pack","brand":"Swanson","sale_price":3.99,"regular_price":6.49,"price_unit":"ea","promo_type":"multi_buy","category_l1":"Pantry","category_l2":"Broths & Soups","deal_score":68,"chain":"zehrs"},
    {"name_en":"Peanut Butter 1 kg","brand":"Kraft","sale_price":6.97,"regular_price":10.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Pantry","category_l2":"Spreads","deal_score":77,"chain":"safeway"},
    {"name_en":"Tomato Pasta Sauce 680 mL","brand":"Rao's Homemade","sale_price":7.49,"regular_price":11.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Pantry","category_l2":"Pasta Sauces","deal_score":72,"chain":"sobeys"},
    {"name_en":"Black Beans 540 mL 4-pack","brand":"PC","sale_price":3.99,"regular_price":6.49,"price_unit":"ea","promo_type":"multi_buy","category_l1":"Pantry","category_l2":"Canned Goods","deal_score":73,"chain":"super_c"},

    # Beverages
    {"name_en":"Orange Juice 1.65 L","brand":"Tropicana","sale_price":4.99,"regular_price":7.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Beverages","category_l2":"Juice","deal_score":70,"chain":"loblaws"},
    {"name_en":"Sparkling Water 12-pack","brand":"Perrier","sale_price":7.99,"regular_price":11.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Beverages","category_l2":"Water","deal_score":67,"chain":"metro"},
    {"name_en":"Coffee Pods 30 ct","brand":"Nespresso","sale_price":16.97,"regular_price":24.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Beverages","category_l2":"Coffee","deal_score":76,"chain":"real_canadian_superstore"},
    {"name_en":"Herbal Tea 20-pack","brand":"Celestial Seasonings","sale_price":3.49,"regular_price":5.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Beverages","category_l2":"Tea","deal_score":63,"chain":"farm_boy"},
    {"name_en":"Apple Juice 2 L","brand":"Mott's","sale_price":3.49,"regular_price":5.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Beverages","category_l2":"Juice","deal_score":64,"chain":"iga"},

    # Snacks
    {"name_en":"Potato Chips 220g","brand":"Lay's","sale_price":2.99,"regular_price":4.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Snacks","category_l2":"Chips","deal_score":59,"chain":"nofrills"},
    {"name_en":"Granola Bars 8-pack","brand":"Nature Valley","sale_price":3.49,"regular_price":5.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Snacks","category_l2":"Bars","deal_score":65,"chain":"walmart"},
    {"name_en":"Trail Mix 600g","brand":"Kirkland","sale_price":8.97,"regular_price":13.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Snacks","category_l2":"Nuts & Dried Fruit","deal_score":74,"chain":"sobeys"},
    {"name_en":"Dark Chocolate Bar 100g","brand":"Lindt","sale_price":2.49,"regular_price":3.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Snacks","category_l2":"Chocolate","deal_score":62,"chain":"iga"},
    {"name_en":"Popcorn 3-pack","brand":"Orville Redenbacher","sale_price":3.99,"regular_price":6.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Snacks","category_l2":"Popcorn","deal_score":58,"chain":"food_basics"},

    # Frozen
    {"name_en":"Frozen Edamame 500g","brand":"","sale_price":2.99,"regular_price":4.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Frozen","category_l2":"Vegetables","deal_score":64,"chain":"food_basics"},
    {"name_en":"Frozen Pizza","brand":"Dr. Oetker","sale_price":5.99,"regular_price":8.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Frozen","category_l2":"Pizza","deal_score":67,"chain":"nofrills"},
    {"name_en":"Ice Cream 1.5 L","brand":"Chapman's","sale_price":4.99,"regular_price":7.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Frozen","category_l2":"Ice Cream","deal_score":69,"chain":"loblaws"},
    {"name_en":"Frozen Fish Fillets 907g","brand":"High Liner","sale_price":11.97,"regular_price":18.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Frozen","category_l2":"Seafood","deal_score":79,"chain":"real_canadian_superstore"},
    {"name_en":"Frozen Mixed Berries 600g","brand":"PC","sale_price":4.49,"regular_price":6.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Frozen","category_l2":"Fruit","deal_score":74,"chain":"metro"},
    {"name_en":"Frozen Burritos 6-pack","brand":"El Monterey","sale_price":6.49,"regular_price":9.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Frozen","category_l2":"Meals","deal_score":61,"chain":"walmart"},

    # Deli & Prepared
    {"name_en":"Rotisserie Chicken","brand":"","sale_price":9.97,"regular_price":13.99,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Deli & Prepared","category_l2":"Prepared Meals","deal_score":80,"chain":"sobeys"},
    {"name_en":"Sliced Turkey Breast 175g","brand":"Maple Leaf","sale_price":3.49,"regular_price":5.49,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Deli & Prepared","category_l2":"Deli Meats","deal_score":67,"chain":"metro"},
    {"name_en":"Hummus 454g","brand":"Fontaine Santé","sale_price":3.99,"regular_price":5.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Deli & Prepared","category_l2":"Dips & Spreads","deal_score":72,"chain":"adonis"},
    {"name_en":"Pepperoni Sticks 500g","brand":"Grimm's","sale_price":7.49,"regular_price":11.99,"price_unit":"ea","promo_type":"percentage_off","category_l1":"Deli & Prepared","category_l2":"Deli Meats","deal_score":71,"chain":"safeway"},
    {"name_en":"Macaroni Salad 750g","brand":"","sale_price":5.49,"regular_price":8.49,"price_unit":"ea","promo_type":"dollar_off","category_l1":"Deli & Prepared","category_l2":"Salads","deal_score":60,"chain":"loblaws"},
]


def confidence_label(score: int) -> str:
    if score >= 75:
        return "High"
    if score >= 60:
        return "Medium"
    return "Low"


def build_records(deals: list) -> list:
    records = []
    for i, d in enumerate(deals):
        chain = d["chain"]
        score = d["deal_score"]
        record = {
            "flyer_id":         REGIONS[chain],
            "sku":              f"MOCK{i + 1000:04d}",
            "store_chain":      chain,
            "store_id":         None,
            "name_en":          d["name_en"],
            "name_fr":          None,
            "brand":            d.get("brand", ""),
            "sale_price":       d["sale_price"],
            "regular_price":    d["regular_price"],
            "price_unit":       d.get("price_unit", "ea"),
            "promo_type":       d["promo_type"],
            "flyer_valid_from": VALID_FROM,
            "flyer_valid_to":   VALID_TO,
            "deal_score":       score,
            "confidence":       round(0.55 + (score - 55) / 200, 2),
            "confidence_label": confidence_label(score),
            "category_l1":      d["category_l1"],
            "category_l2":      d.get("category_l2", ""),
            "image_url":        None,
        }
        records.append(record)
    return records


if __name__ == "__main__":
    records = build_records(DEALS)

    out = Path("frontend/public/data/active_scores.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    chains: dict[str, int] = {}
    for r in records:
        chains[r["store_chain"]] = chains.get(r["store_chain"], 0) + 1

    print(f"Wrote {len(records)} mock deals to {out}")
    for c, n in sorted(chains.items()):
        print(f"  {c}: {n} deals")
