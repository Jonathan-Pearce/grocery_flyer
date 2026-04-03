"""
Unified output schema for cleaned grocery flyer items.

All flyer records — whether sourced from the Flipp FlyerKit API or the
Metro Digital Azure API — are normalised into a ``FlyerItem`` instance
before any downstream processing.

Usage::

    item = FlyerItem(
        source_api="flipp",
        store_chain="loblaws",
        store_id="1234",
        flyer_id="567890",
    )
    json_str = item.model_dump_json()
    restored = FlyerItem.model_validate_json(json_str)
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class FlyerItem(BaseModel):
    """One cleaned product observation from a grocery flyer."""

    model_config = ConfigDict(extra="ignore")

    # ── Source / provenance ───────────────────────────────────────────────────

    source_api: Literal["flipp", "metro"] | None = None
    """Which upstream API supplied this record."""

    store_chain: str | None = None
    """Normalised chain slug, e.g. ``"loblaws"``, ``"food_basics"``."""

    store_id: str | None = None
    """Raw store identifier as returned by the source API."""

    flyer_id: str | None = None
    """Publication ID (Flipp) or job number (Metro)."""

    flyer_valid_from: str | None = None
    """ISO 8601 date on which the flyer pricing becomes valid."""

    flyer_valid_to: str | None = None
    """ISO 8601 date on which the flyer pricing expires."""

    fetched_on: str | None = None
    """ISO 8601 date the raw file was downloaded (for cache-staleness audits)."""

    province: str | None = None
    """Two-letter Canadian province code, e.g. ``"ON"``, ``"QC"``."""

    # ── Raw originals (audit / debugging) ────────────────────────────────────

    raw_name: str | None = None
    """Product name exactly as it appears in the source payload."""

    raw_description: str | None = None
    """Product description exactly as it appears in the source payload."""

    raw_body: str | None = None
    """Full promotional body text from the source payload."""

    pre_price_text: str | None = None
    """Text preceding the price, e.g. ``"from"`` or ``"starting at"``."""

    post_price_text: str | None = None
    """Text following the price, e.g. ``"+TX"`` or ``"each"``."""

    raw_categories: list[str] | None = None
    """Category tags exactly as received from the source API."""

    # ── Cleaned product identity ──────────────────────────────────────────────

    name_en: str | None = None
    """Cleaned English product name."""

    name_fr: str | None = None
    """Cleaned French product name."""

    description_en: str | None = None
    """Cleaned English product description."""

    description_fr: str | None = None
    """Cleaned French product description."""

    brand: str | None = None
    """Manufacturer / brand name."""

    sku: str | None = None
    """Stock-keeping unit or item code."""

    language: Literal["en", "fr", "bil"] | None = None
    """Dominant language of the source record: English, French, or bilingual."""

    product_url: str | None = None
    """Canonical URL for the product page."""

    image_url: str | None = None
    """URL of the product image."""

    # ── Prices ────────────────────────────────────────────────────────────────

    sale_price: float | None = None
    """Advertised sale / flyer price."""

    regular_price: float | None = None
    """Regular (non-promotional) shelf price."""

    price_unit: str | None = None
    """Unit the price applies to, e.g. ``"each"``, ``"lb"``, ``"kg"``."""

    price_per_kg: float | None = None
    """Normalised price per kilogram for weight-based comparisons."""

    price_per_lb: float | None = None
    """Normalised price per pound for weight-based comparisons."""

    alternate_price: float | None = None
    """Secondary price shown alongside the main price (e.g. per-unit price)."""

    alternate_unit: str | None = None
    """Unit for ``alternate_price``."""

    member_price: float | None = None
    """Loyalty / membership programme price, when distinct from ``sale_price``."""

    price_is_floor: bool = False
    """``True`` when the price is prefixed with "starting at" / "from"."""

    multi_buy_qty: int | None = None
    """Required purchase quantity for a multi-buy deal, e.g. ``2`` in "2/$5"."""

    multi_buy_total: float | None = None
    """Total price for the multi-buy deal, e.g. ``5.00`` in "2/$5"."""

    currency: str = "CAD"
    """ISO 4217 currency code; always ``"CAD"`` for Canadian flyers."""

    purchase_limit: int | None = None
    """Maximum units a customer may purchase at the promotional price."""

    over_limit_price: float | None = None
    """Price per unit for quantities above ``purchase_limit``."""

    tax_indicator: str | None = None
    """Tax flag exactly as shown, e.g. ``"+TX"``."""

    # ── Promo ─────────────────────────────────────────────────────────────────

    promo_type: Literal[
        "rollback",
        "percentage_off",
        "dollar_off",
        "multi_buy",
        "bogo",
        "loyalty_points",
        "member_price",
        "clearance",
        "no_promo",
    ] | None = None
    """Normalised promotion category."""

    promo_details: str | None = None
    """Raw promotional string as it appears in the source, for reference."""

    loyalty_program: Literal["Scene+", "PC Optimum"] | None = None
    """Loyalty programme associated with the promotion, if any."""

    loyalty_points: int | None = None
    """Number of loyalty points earned on this item."""

    loyalty_trigger: str | None = None
    """Condition for earning points, e.g. ``"when you buy 2"``."""

    # ── Weight / size ─────────────────────────────────────────────────────────

    weight_value: float | None = None
    """Primary package weight or volume (numeric part only)."""

    weight_unit: Literal["g", "kg", "mL", "L", "lb", "oz", "count"] | None = None
    """Unit for ``weight_value``."""

    weight_is_range: bool = False
    """``True`` when the package size is expressed as a range."""

    weight_min: float | None = None
    """Lower bound of the weight range (when ``weight_is_range`` is ``True``)."""

    weight_max: float | None = None
    """Upper bound of the weight range (when ``weight_is_range`` is ``True``)."""

    pack_count: int | None = None
    """Number of individual units in a multipack, e.g. ``6`` from "6×355 mL"."""

    pack_unit_size: float | None = None
    """Size of each unit in a multipack, e.g. ``355.0`` from "6×355 mL"."""

    pack_unit: str | None = None
    """Unit for ``pack_unit_size``, e.g. ``"mL"``."""

    # ── Categories (harmonised) ───────────────────────────────────────────────

    category_l1: str | None = None
    """Top-level Google product taxonomy category."""

    category_l2: str | None = None
    """Second-level Google product taxonomy category."""

    category_l3: str | None = None
    """Third-level Google product taxonomy category."""

    category_l4: str | None = None
    """Fourth-level Google product taxonomy category."""

    is_food: bool = False
    """``True`` when the product is classified as food (including pet food)."""

    is_human_food: bool = False
    """``True`` when the product is food intended for human consumption."""

    # ── Multi-product ─────────────────────────────────────────────────────────

    is_multi_product: bool = False
    """``True`` when a single flyer entry covers more than one distinct product."""

    parent_record_id: str | None = None
    """Links a split child record back to its combined source record."""

    multi_product_variants: list[str] = Field(default_factory=list)
    """Names or SKUs of the individual products when not yet split."""

    # ── Tracking key ─────────────────────────────────────────────────────────

    price_observation_key: str | None = None
    """Composite deduplication key: ``{store_chain}:{store_id}:{sku}:{flyer_valid_from}``."""
