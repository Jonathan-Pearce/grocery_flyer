"""Tests for categories/category_map.py — category harmonisation mapping."""

from __future__ import annotations


from categories.category_map import (
    FOOD_L1,
    GOOGLE_TAXONOMY_L1_MAP,
    HUMAN_FOOD_L1,
    METRO_CATEGORY_MAP,
    METRO_CATEGORY_MAP_FR,
    TARGET_L1,
    get_food_flags,
    map_google_taxonomy,
    map_metro_category,
)


# ── TARGET_L1 ─────────────────────────────────────────────────────────────────


class TestTargetL1:
    def test_contains_all_expected_labels(self):
        expected = {
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
        assert expected == set(TARGET_L1)

    def test_is_frozenset(self):
        assert isinstance(TARGET_L1, frozenset)


# ── METRO_CATEGORY_MAP ────────────────────────────────────────────────────────


class TestMetroCategoryMap:
    def test_all_values_in_target_l1(self):
        for key, val in METRO_CATEGORY_MAP.items():
            assert val in TARGET_L1, (
                f"METRO_CATEGORY_MAP[{key!r}] = {val!r} not in TARGET_L1"
            )

    def test_fruit_and_vegetables_maps_to_produce(self):
        assert METRO_CATEGORY_MAP["Fruit and Vegetables"] == "Produce"

    def test_meat_and_deli_maps_to_meat_seafood(self):
        assert METRO_CATEGORY_MAP["Meat and Deli"] == "Meat & Seafood"

    def test_fish_and_seafood_maps_to_meat_seafood(self):
        assert METRO_CATEGORY_MAP["Fish and Seafood"] == "Meat & Seafood"

    def test_dairy_and_cheese_maps_to_dairy_eggs(self):
        assert METRO_CATEGORY_MAP["Dairy and Cheese"] == "Dairy & Eggs"

    def test_bread_and_bakery_maps_to_bakery(self):
        assert METRO_CATEGORY_MAP["Bread and Bakery Products"] == "Bakery"

    def test_grocery_maps_to_pantry(self):
        assert METRO_CATEGORY_MAP["Grocery"] == "Pantry"

    def test_frozen_food_maps_to_frozen(self):
        assert METRO_CATEGORY_MAP["Frozen Food"] == "Frozen"

    def test_beverages_maps_to_beverages(self):
        assert METRO_CATEGORY_MAP["Beverages"] == "Beverages"

    def test_snacks_maps_to_snacks_confectionery(self):
        assert METRO_CATEGORY_MAP["Snacks"] == "Snacks & Confectionery"

    def test_prepared_meals_maps_to_deli(self):
        assert METRO_CATEGORY_MAP["Prepared Meals"] == "Deli & Prepared Foods"

    def test_baby_health_beauty_maps_to_health_beauty(self):
        assert METRO_CATEGORY_MAP["Baby Products, Health and Beauty"] == "Health & Beauty"

    def test_pharmacy_maps_to_health_beauty(self):
        assert METRO_CATEGORY_MAP["Pharmacy"] == "Health & Beauty"

    def test_household_products_maps_to_household(self):
        assert METRO_CATEGORY_MAP["Household Products"] == "Household"

    def test_pets_maps_to_pet(self):
        assert METRO_CATEGORY_MAP["Pets"] == "Pet"

    def test_other_maps_to_other(self):
        assert METRO_CATEGORY_MAP["Other"] == "Other"

    def test_mq_placeholder_maps_to_other(self):
        """Metro QC's 'MQ' placeholder must map to Other."""
        assert METRO_CATEGORY_MAP["MQ"] == "Other"

    def test_covers_observed_en_categories(self):
        """Every EN category observed in real data must be covered."""
        observed = {
            "Baby Products, Health and Beauty",
            "Beverages",
            "Bread and Bakery Products",
            "Dairy and Cheese",
            "Fish and Seafood",
            "Frozen Food",
            "Fruit and Vegetables",
            "Grocery",
            "Household Products",
            "Meat and Deli",
            "MQ",
            "Other",
            "Pets",
            "Pharmacy",
            "Prepared Meals",
            "Snacks",
        }
        missing = observed - set(METRO_CATEGORY_MAP.keys())
        assert not missing, f"Missing mappings for observed EN categories: {missing}"


# ── METRO_CATEGORY_MAP_FR ─────────────────────────────────────────────────────


class TestMetroCategoryMapFr:
    def test_all_values_in_target_l1(self):
        for key, val in METRO_CATEGORY_MAP_FR.items():
            assert val in TARGET_L1, (
                f"METRO_CATEGORY_MAP_FR[{key!r}] = {val!r} not in TARGET_L1"
            )

    def test_fruits_et_legumes_maps_to_produce(self):
        assert METRO_CATEGORY_MAP_FR["Fruits et légumes"] == "Produce"

    def test_viandes_maps_to_meat_seafood(self):
        assert METRO_CATEGORY_MAP_FR["Viandes et charcuterie"] == "Meat & Seafood"

    def test_poissons_maps_to_meat_seafood(self):
        assert METRO_CATEGORY_MAP_FR["Poissons et fruits de mer"] == "Meat & Seafood"

    def test_fromage_maps_to_dairy_eggs(self):
        assert METRO_CATEGORY_MAP_FR["Fromage et produits laitiers"] == "Dairy & Eggs"

    def test_boulangerie_maps_to_bakery(self):
        assert METRO_CATEGORY_MAP_FR["Boulangerie et pâtisserie"] == "Bakery"

    def test_epicerie_maps_to_pantry(self):
        assert METRO_CATEGORY_MAP_FR["Épicerie"] == "Pantry"

    def test_surgeles_maps_to_frozen(self):
        assert METRO_CATEGORY_MAP_FR["Produits surgelés"] == "Frozen"

    def test_jus_maps_to_beverages(self):
        assert METRO_CATEGORY_MAP_FR["Jus et rafraîchissements"] == "Beverages"

    def test_collations_maps_to_snacks(self):
        assert METRO_CATEGORY_MAP_FR["Collations"] == "Snacks & Confectionery"

    def test_mets_prepares_maps_to_deli(self):
        assert METRO_CATEGORY_MAP_FR["Mets préparés et rôtisserie"] == "Deli & Prepared Foods"

    def test_charcuterie_et_plats_cuisines_maps_to_deli(self):
        assert METRO_CATEGORY_MAP_FR["Charcuterie et Plats Cuisinés"] == "Deli & Prepared Foods"

    def test_garde_manger_maps_to_pantry(self):
        assert METRO_CATEGORY_MAP_FR["Garde-Manger"] == "Pantry"

    def test_bebe_sante_beaute_maps_to_health_beauty(self):
        assert METRO_CATEGORY_MAP_FR["Bébé, Santé beauté"] == "Health & Beauty"

    def test_pharmacie_maps_to_health_beauty(self):
        assert METRO_CATEGORY_MAP_FR["Pharmacie"] == "Health & Beauty"

    def test_produits_menagers_maps_to_household(self):
        assert METRO_CATEGORY_MAP_FR["Produits ménagers"] == "Household"

    def test_animaux_maps_to_pet(self):
        assert METRO_CATEGORY_MAP_FR["Animaux"] == "Pet"

    def test_divers_maps_to_other(self):
        assert METRO_CATEGORY_MAP_FR["Divers"] == "Other"

    def test_covers_observed_fr_categories(self):
        """Every FR category observed in real data must be covered."""
        observed = {
            "Animaux",
            "Boulangerie et pâtisserie",
            "Bébé, Santé beauté",
            "Collations",
            "Divers",
            "Fromage et produits laitiers",
            "Fruits et légumes",
            "Jus et rafraîchissements",
            "Mets préparés et rôtisserie",
            "Pharmacie",
            "Poissons et fruits de mer",
            "Produits ménagers",
            "Produits surgelés",
            "Viandes et charcuterie",
            "Épicerie",
        }
        missing = observed - set(METRO_CATEGORY_MAP_FR.keys())
        assert not missing, f"Missing mappings for observed FR categories: {missing}"


# ── GOOGLE_TAXONOMY_L1_MAP ────────────────────────────────────────────────────


class TestGoogleTaxonomyL1Map:
    def test_all_non_none_values_in_target_l1(self):
        for key, val in GOOGLE_TAXONOMY_L1_MAP.items():
            if val is not None:
                assert val in TARGET_L1, (
                    f"GOOGLE_TAXONOMY_L1_MAP[{key!r}] = {val!r} not in TARGET_L1"
                )

    def test_food_beverages_tobacco_maps_to_none(self):
        """'Food, Beverages & Tobacco' requires L2 disambiguation → None."""
        assert GOOGLE_TAXONOMY_L1_MAP["Food, Beverages & Tobacco"] is None

    def test_health_and_beauty_maps_correctly(self):
        assert GOOGLE_TAXONOMY_L1_MAP["Health & Beauty"] == "Health & Beauty"

    def test_animals_and_pet_supplies_maps_to_pet(self):
        assert GOOGLE_TAXONOMY_L1_MAP["Animals & Pet Supplies"] == "Pet"

    def test_baby_and_toddler_maps_to_baby_infant(self):
        assert GOOGLE_TAXONOMY_L1_MAP["Baby & Toddler"] == "Baby & Infant"

    def test_sporting_goods_maps_to_apparel_gm(self):
        assert GOOGLE_TAXONOMY_L1_MAP["Sporting Goods"] == "Apparel & General Merchandise"

    def test_home_and_garden_maps_to_household(self):
        assert GOOGLE_TAXONOMY_L1_MAP["Home & Garden"] == "Household"

    def test_covers_observed_l1_categories(self):
        """Every L1 observed in real Flipp data must be covered."""
        observed = {
            "Animals & Pet Supplies",
            "Apparel & Accessories",
            "Arts & Entertainment",
            "Baby & Toddler",
            "Business & Industrial",
            "Electronics",
            "Food, Beverages & Tobacco",
            "Furniture",
            "Hardware",
            "Health & Beauty",
            "Home & Garden",
            "Luggage & Bags",
            "Mature",
            "Media",
            "Office Supplies",
            "Software",
            "Sporting Goods",
            "Toys & Games",
            "Vehicles & Parts",
        }
        missing = observed - set(GOOGLE_TAXONOMY_L1_MAP.keys())
        assert not missing, f"Missing L1 mappings for observed categories: {missing}"


# ── FOOD_L1 / HUMAN_FOOD_L1 ───────────────────────────────────────────────────


class TestFoodSets:
    def test_food_l1_is_frozenset(self):
        assert isinstance(FOOD_L1, frozenset)

    def test_human_food_l1_is_frozenset(self):
        assert isinstance(HUMAN_FOOD_L1, frozenset)

    def test_human_food_is_subset_of_food(self):
        assert HUMAN_FOOD_L1 < FOOD_L1

    def test_pet_in_food_but_not_human_food(self):
        assert "Pet" in FOOD_L1
        assert "Pet" not in HUMAN_FOOD_L1

    def test_produce_in_both(self):
        assert "Produce" in FOOD_L1
        assert "Produce" in HUMAN_FOOD_L1

    def test_household_not_in_food(self):
        assert "Household" not in FOOD_L1
        assert "Household" not in HUMAN_FOOD_L1

    def test_apparel_not_in_food(self):
        assert "Apparel & General Merchandise" not in FOOD_L1

    def test_all_food_core_labels_present(self):
        core_food = {
            "Produce",
            "Meat & Seafood",
            "Dairy & Eggs",
            "Bakery",
            "Pantry",
            "Frozen",
            "Beverages",
            "Snacks & Confectionery",
            "Deli & Prepared Foods",
        }
        assert core_food <= FOOD_L1


# ── map_metro_category ────────────────────────────────────────────────────────


class TestMapMetroCategory:
    def test_en_maps_correctly(self):
        assert map_metro_category("Fruit and Vegetables", None) == "Produce"

    def test_en_preferred_over_fr(self):
        assert map_metro_category("Meat and Deli", "Viandes et charcuterie") == "Meat & Seafood"

    def test_fr_fallback_when_en_none(self):
        assert map_metro_category(None, "Fruits et légumes") == "Produce"

    def test_fr_fallback_when_en_empty_string(self):
        assert map_metro_category("", "Fruits et légumes") == "Produce"

    def test_both_none_returns_none(self):
        assert map_metro_category(None, None) is None

    def test_unknown_en_falls_back_to_fr(self):
        """When EN is present but unmapped, fall back to the French value."""
        assert map_metro_category("SomeUnknownCategory", "Épicerie") == "Pantry"

    def test_whitespace_stripped_en(self):
        assert map_metro_category("  Grocery  ", None) == "Pantry"

    def test_whitespace_stripped_fr(self):
        assert map_metro_category(None, "  Épicerie  ") == "Pantry"

    def test_pharmacy_en(self):
        assert map_metro_category("Pharmacy", None) == "Health & Beauty"

    def test_pets_en(self):
        assert map_metro_category("Pets", None) == "Pet"

    def test_prepared_meals_en(self):
        assert map_metro_category("Prepared Meals", None) == "Deli & Prepared Foods"

    def test_frozen_food_en(self):
        assert map_metro_category("Frozen Food", None) == "Frozen"

    def test_dairy_and_cheese_en(self):
        assert map_metro_category("Dairy and Cheese", None) == "Dairy & Eggs"

    def test_snacks_en(self):
        assert map_metro_category("Snacks", None) == "Snacks & Confectionery"


# ── map_google_taxonomy ───────────────────────────────────────────────────────


class TestMapGoogleTaxonomy:
    def test_health_and_beauty_l1_only(self):
        assert map_google_taxonomy("Health & Beauty") == "Health & Beauty"

    def test_animals_l1_only(self):
        assert map_google_taxonomy("Animals & Pet Supplies") == "Pet"

    def test_baby_and_toddler_l1_only(self):
        assert map_google_taxonomy("Baby & Toddler") == "Baby & Infant"

    def test_food_with_food_items_l2(self):
        assert map_google_taxonomy("Food, Beverages & Tobacco", "Food Items") == "Pantry"

    def test_food_with_beverages_l2(self):
        assert map_google_taxonomy("Food, Beverages & Tobacco", "Beverages") == "Beverages"

    def test_food_with_tobacco_l2(self):
        assert map_google_taxonomy("Food, Beverages & Tobacco", "Tobacco Products") == "Other"

    def test_food_no_l2_defaults_to_pantry(self):
        assert map_google_taxonomy("Food, Beverages & Tobacco") == "Pantry"

    def test_food_unknown_l2_defaults_to_pantry(self):
        assert map_google_taxonomy("Food, Beverages & Tobacco", "Unknown L2") == "Pantry"

    def test_home_and_garden(self):
        assert map_google_taxonomy("Home & Garden") == "Household"

    def test_sporting_goods(self):
        assert map_google_taxonomy("Sporting Goods") == "Apparel & General Merchandise"

    def test_none_l1_returns_none(self):
        assert map_google_taxonomy(None) is None

    def test_empty_l1_returns_none(self):
        assert map_google_taxonomy("") is None

    def test_unknown_l1_returns_none(self):
        assert map_google_taxonomy("Completely Unknown L1") is None

    def test_electronics_maps_to_apparel_gm(self):
        assert map_google_taxonomy("Electronics") == "Apparel & General Merchandise"

    def test_furniture_maps_to_household(self):
        assert map_google_taxonomy("Furniture") == "Household"

    def test_hardware_maps_to_household(self):
        assert map_google_taxonomy("Hardware") == "Household"

    def test_toys_and_games_maps_to_apparel_gm(self):
        assert map_google_taxonomy("Toys & Games") == "Apparel & General Merchandise"

    def test_apparel_accessories_maps_to_apparel_gm(self):
        assert map_google_taxonomy("Apparel & Accessories") == "Apparel & General Merchandise"


# ── get_food_flags ────────────────────────────────────────────────────────────


class TestGetFoodFlags:
    def test_produce_is_food_and_human_food(self):
        assert get_food_flags("Produce") == (True, True)

    def test_meat_seafood_is_food_and_human_food(self):
        assert get_food_flags("Meat & Seafood") == (True, True)

    def test_dairy_eggs_is_food_and_human_food(self):
        assert get_food_flags("Dairy & Eggs") == (True, True)

    def test_bakery_is_food_and_human_food(self):
        assert get_food_flags("Bakery") == (True, True)

    def test_pantry_is_food_and_human_food(self):
        assert get_food_flags("Pantry") == (True, True)

    def test_frozen_is_food_and_human_food(self):
        assert get_food_flags("Frozen") == (True, True)

    def test_beverages_is_food_and_human_food(self):
        assert get_food_flags("Beverages") == (True, True)

    def test_snacks_confectionery_is_food_and_human_food(self):
        assert get_food_flags("Snacks & Confectionery") == (True, True)

    def test_deli_prepared_foods_is_food_and_human_food(self):
        assert get_food_flags("Deli & Prepared Foods") == (True, True)

    def test_pet_is_food_not_human_food(self):
        """Pet food: is_food=True, is_human_food=False."""
        assert get_food_flags("Pet") == (True, False)

    def test_household_is_not_food(self):
        assert get_food_flags("Household") == (False, False)

    def test_health_beauty_is_not_food(self):
        assert get_food_flags("Health & Beauty") == (False, False)

    def test_apparel_gm_is_not_food(self):
        assert get_food_flags("Apparel & General Merchandise") == (False, False)

    def test_baby_infant_is_not_food(self):
        """Baby & Infant (non-food baby items) should not be classified as food."""
        assert get_food_flags("Baby & Infant") == (False, False)

    def test_other_is_not_food(self):
        assert get_food_flags("Other") == (False, False)

    def test_none_is_not_food(self):
        assert get_food_flags(None) == (False, False)

    def test_unknown_string_is_not_food(self):
        assert get_food_flags("Unknown Category") == (False, False)

    def test_bike_dog_toy_scenario(self):
        """Non-food items like bikes and dog toys map to non-food categories."""
        # A bicycle would be "Apparel & General Merchandise" from Sporting Goods
        assert get_food_flags("Apparel & General Merchandise") == (False, False)
        # A dog toy would be "Pet" — is_food=True because it's a pet product
        # but is_human_food=False
        is_food, is_human_food = get_food_flags("Pet")
        assert is_food is True
        assert is_human_food is False
