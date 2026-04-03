"""
Category harmonisation mapping for grocery flyers.

Provides dictionaries and helpers that map raw category strings from both the
Flipp FlyerKit API (Google taxonomy) and the Metro Digital Azure API into a
shared, consistent top-level grocery taxonomy.

Target taxonomy (L1 labels)
----------------------------
Produce
Meat & Seafood
Dairy & Eggs
Bakery
Pantry
Frozen
Beverages
Snacks & Confectionery
Deli & Prepared Foods
Health & Beauty
Household
Pet
Baby & Infant
Apparel & General Merchandise
Other

Usage::

    from categories.category_map import (
        map_metro_category,
        map_google_taxonomy,
        get_food_flags,
    )

    # Metro (English first, French fallback)
    l1 = map_metro_category("Fruit and Vegetables", None)  # → "Produce"
    l1 = map_metro_category(None, "Fruits et légumes")     # → "Produce"

    # Flipp Google taxonomy
    l1 = map_google_taxonomy("Health & Beauty", None)          # → "Health & Beauty"
    l1 = map_google_taxonomy("Food, Beverages & Tobacco", "Food Items")  # → "Pantry"

    # Food flags
    is_food, is_human_food = get_food_flags("Produce")         # → (True, True)
    is_food, is_human_food = get_food_flags("Pet")             # → (True, False)
    is_food, is_human_food = get_food_flags("Household")       # → (False, False)
"""

from __future__ import annotations

# ── Target L1 taxonomy ────────────────────────────────────────────────────────

TARGET_L1: frozenset[str] = frozenset(
    {
        "Produce",
        "Meat & Seafood",
        "Dairy & Eggs",
        "Bakery",
        "Pantry",
        "Frozen",
        "Beverages",
        "Snacks & Confectionery",
        "Deli & Prepared Foods",
        "Health & Beauty",
        "Household",
        "Pet",
        "Baby & Infant",
        "Apparel & General Merchandise",
        "Other",
    }
)

# ── Food flags ────────────────────────────────────────────────────────────────

#: Categories classified as food (including pet food).
FOOD_L1: frozenset[str] = frozenset(
    {
        "Produce",
        "Meat & Seafood",
        "Dairy & Eggs",
        "Bakery",
        "Pantry",
        "Frozen",
        "Beverages",
        "Snacks & Confectionery",
        "Deli & Prepared Foods",
        "Pet",
    }
)

#: Subset of FOOD_L1 that is food intended for human consumption (excludes Pet).
HUMAN_FOOD_L1: frozenset[str] = FOOD_L1 - {"Pet"}

# ── Metro English category map ────────────────────────────────────────────────

#: Maps Metro ``mainCategoryEn`` strings to a target L1 label.
METRO_CATEGORY_MAP: dict[str, str] = {
    "Fruit and Vegetables": "Produce",
    "Meat and Deli": "Meat & Seafood",
    "Fish and Seafood": "Meat & Seafood",
    "Dairy and Cheese": "Dairy & Eggs",
    "Bread and Bakery Products": "Bakery",
    "Grocery": "Pantry",
    "Frozen Food": "Frozen",
    "Beverages": "Beverages",
    "Snacks": "Snacks & Confectionery",
    "Prepared Meals": "Deli & Prepared Foods",
    "Charcuterie and Ready-made Meals": "Deli & Prepared Foods",
    "Baby Products, Health and Beauty": "Health & Beauty",
    "Pharmacy": "Health & Beauty",
    "Household Products": "Household",
    "Pets": "Pet",
    # Metro QC uses "MQ" as a placeholder / catch-all
    "MQ": "Other",
    "Other": "Other",
}

# ── Metro French category map ─────────────────────────────────────────────────

#: Maps Metro ``mainCategoryFr`` strings to a target L1 label.
#: Used as a fallback when ``mainCategoryEn`` is absent or empty.
METRO_CATEGORY_MAP_FR: dict[str, str] = {
    "Fruits et légumes": "Produce",
    "Viandes et charcuterie": "Meat & Seafood",
    "Poissons et fruits de mer": "Meat & Seafood",
    "Fromage et produits laitiers": "Dairy & Eggs",
    "Boulangerie et pâtisserie": "Bakery",
    "Épicerie": "Pantry",
    "Produits surgelés": "Frozen",
    "Jus et rafraîchissements": "Beverages",
    "Collations": "Snacks & Confectionery",
    "Mets préparés et rôtisserie": "Deli & Prepared Foods",
    "Charcuterie et Plats Cuisinés": "Deli & Prepared Foods",
    "Garde-Manger": "Pantry",
    "Bébé, Santé beauté": "Health & Beauty",
    "Pharmacie": "Health & Beauty",
    "Produits ménagers": "Household",
    "Animaux": "Pet",
    "Divers": "Other",
}

# ── Flipp Google taxonomy L1 map ──────────────────────────────────────────────

#: Maps Flipp ``item_categories.l1.category_name`` to a target L1 label.
#: A value of ``None`` signals that the L2 category must be consulted to
#: resolve the final label (see :func:`map_google_taxonomy`).
GOOGLE_TAXONOMY_L1_MAP: dict[str, str | None] = {
    # Requires L2 disambiguation (Food Items → Pantry, Beverages → Beverages, …)
    "Food, Beverages & Tobacco": None,
    # Direct mappings
    "Health & Beauty": "Health & Beauty",
    "Animals & Pet Supplies": "Pet",
    "Baby & Toddler": "Baby & Infant",
    "Sporting Goods": "Apparel & General Merchandise",
    "Home & Garden": "Household",
    "Apparel & Accessories": "Apparel & General Merchandise",
    "Toys & Games": "Apparel & General Merchandise",
    "Electronics": "Apparel & General Merchandise",
    "Furniture": "Household",
    "Hardware": "Household",
    "Office Supplies": "Apparel & General Merchandise",
    "Media": "Apparel & General Merchandise",
    "Software": "Apparel & General Merchandise",
    "Vehicles & Parts": "Apparel & General Merchandise",
    "Business & Industrial": "Apparel & General Merchandise",
    "Arts & Entertainment": "Apparel & General Merchandise",
    "Luggage & Bags": "Apparel & General Merchandise",
    "Mature": "Other",
}

#: When L1 is ``"Food, Beverages & Tobacco"``, map L2 to a target L1 label.
GOOGLE_TAXONOMY_L2_FOOD_MAP: dict[str, str] = {
    "Food Items": "Pantry",
    "Food Service": "Pantry",
    "Beverages": "Beverages",
    "Tobacco Products": "Other",
}

# ── Public helpers ────────────────────────────────────────────────────────────


def map_metro_category(
    main_en: str | None,
    main_fr: str | None,
) -> str | None:
    """Resolve a Metro product's raw category strings to a target L1 label.

    English is preferred; French is used as a fallback when English is absent
    or empty.  Returns ``None`` when neither can be mapped.

    Parameters
    ----------
    main_en:
        Value of ``mainCategoryEn`` from the Metro product payload.
    main_fr:
        Value of ``mainCategoryFr`` from the Metro product payload.
    """
    en = (main_en or "").strip()
    if en:
        result = METRO_CATEGORY_MAP.get(en)
        if result is not None:
            return result

    fr = (main_fr or "").strip()
    if fr:
        return METRO_CATEGORY_MAP_FR.get(fr)

    return None


def map_google_taxonomy(
    l1_name: str | None,
    l2_name: str | None = None,
) -> str | None:
    """Resolve a Flipp Google taxonomy L1 (and optionally L2) to a target L1.

    When ``l1_name`` maps to ``None`` in :data:`GOOGLE_TAXONOMY_L1_MAP` (i.e.
    ``"Food, Beverages & Tobacco"``), ``l2_name`` is used for further
    disambiguation via :data:`GOOGLE_TAXONOMY_L2_FOOD_MAP`.

    Parameters
    ----------
    l1_name:
        Value of ``item_categories.l1.category_name`` from a Flipp product.
    l2_name:
        Value of ``item_categories.l2.category_name`` from the same product,
        used when ``l1_name`` alone is insufficient.
    """
    if not l1_name:
        return None

    l1_result = GOOGLE_TAXONOMY_L1_MAP.get(l1_name)
    if l1_result is not None:
        return l1_result

    # l1_result is None — either the key is absent, or it maps to None (L2 needed)
    if l1_name in GOOGLE_TAXONOMY_L1_MAP:
        # The L1 key exists but requires L2 disambiguation
        if l2_name:
            return GOOGLE_TAXONOMY_L2_FOOD_MAP.get(l2_name, "Pantry")
        return "Pantry"

    # Unknown L1 — return None to signal unmapped
    return None


def get_food_flags(category_l1: str | None) -> tuple[bool, bool]:
    """Return ``(is_food, is_human_food)`` flags for a harmonised L1 label.

    Parameters
    ----------
    category_l1:
        A target-taxonomy L1 label, e.g. ``"Produce"``, ``"Pet"``, ``"Household"``.

    Returns
    -------
    tuple[bool, bool]
        ``(is_food, is_human_food)`` where:

        * ``is_food`` is ``True`` for food items including pet food.
        * ``is_human_food`` is ``True`` only for food intended for humans
          (i.e. ``is_food`` is ``True`` and the category is not ``"Pet"``).
    """
    if category_l1 is None:
        return False, False
    is_food = category_l1 in FOOD_L1
    is_human_food = category_l1 in HUMAN_FOOD_L1
    return is_food, is_human_food
