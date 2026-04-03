"""
Shared utilities for the Metro Azure API scrapers.

Defines the MetroBrand dataclass, METRO_PORTFOLIO config, and all HTTP
helper functions so fetch_stores.py and fetch_flyers.py stay thin.
"""

from dataclasses import dataclass

import requests

METRO_API_BASE = "https://metrodigital-apim.azure-api.net/api"


# ── Brand config ──────────────────────────────────────────────────────────────

@dataclass
class MetroBrand:
    name: str
    folder: str
    app_config_url: str   # SPA app.json URL; used to fetch banner_id/api_key if None
    banner_id: str | None  # None = fetched from app_config_url at runtime
    api_key: str | None    # None = fetched from app_config_url at runtime
    id_range: range        # Integer store-ID sweep range
    expected_banner: str | None = None  # If set, only stores whose API banner matches are kept
    locale: str = "en"    # API locale: "en" for ON brands, "fr" for QC brands


# banner_id/api_key for Metro Ontario are hardcoded (known from app.3a018fde.js).
# Other brands have None and will be populated by metro_load_credentials() at runtime.
METRO_PORTFOLIO: list[MetroBrand] = [
    MetroBrand(
        name="Metro Ontario",
        folder="metro",
        app_config_url="https://flyer.metro.ca/config/app.json",
        banner_id="62e3eddbffe0e6f10778a56d",
        api_key="021027e7c41548bcba5d2315a155816b",
        id_range=range(1, 1000),
    ),
    MetroBrand(
        name="Food Basics",
        folder="food_basics",
        app_config_url="https://flyer.foodbasics.ca/config/app.json",
        banner_id="62015981ed29a2a604a206b4",
        api_key="0defd42b9de9412488327864774fbfca",
        id_range=range(1, 1000),
    ),
    MetroBrand(
        name="Adonis",
        folder="adonis",
        app_config_url="https://depliant.adonis.ca/config/app.json",
        banner_id="63fe18ec3e7cd81e86393c61",
        api_key="0a112db32b2f42588b54063b05dfbc90",
        id_range=range(1, 30000),
        locale="fr",
    ),
    MetroBrand(
        name="Super C",
        folder="super_c",
        app_config_url="https://depliant.superc.ca/config/app.json",
        banner_id="6141fa7157f8c212fc19dddc",
        api_key="021027e7c41548bcba5d2315a155816b",
        id_range=range(1, 1000),
        locale="fr",
    ),
    # Metro Quebec runs on circulaire.metro.ca (separate from depliant.metro.ca which is Super C).
    # banner_id/api_key confirmed from circulaire.metro.ca/config/app.json.
    MetroBrand(
        name="Metro Quebec",
        folder="metro_qc",
        app_config_url="https://circulaire.metro.ca/config/app.json",
        banner_id="62e3ee07ffe0e6f10778a56e",
        api_key="0a112db32b2f42588b54063b05dfbc90",
        id_range=range(1, 1000),
    ),
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def metro_load_credentials(brand: MetroBrand) -> bool:
    """Fetch app.json to populate banner_id/api_key if not hardcoded.

    Returns True if credentials are available after the attempt.
    """
    if brand.banner_id and brand.api_key:
        return True
    try:
        r = requests.get(
            brand.app_config_url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        r.raise_for_status()
        cfg = r.json()
        brand.banner_id = cfg.get("banner_id") or brand.banner_id
        brand.api_key   = cfg.get("apikey")    or brand.api_key
    except Exception as exc:
        print(f"  [!] Could not load app.json for {brand.name}: {exc}")
    return bool(brand.banner_id and brand.api_key)


def metro_headers(brand: MetroBrand) -> dict:
    return {
        "Ocp-Apim-Subscription-Key": brand.api_key,
        "Banner": brand.banner_id,
        "User-Agent": "Mozilla/5.0",
    }


def metro_fetch_store(brand: MetroBrand, store_id: int, date: str) -> dict | None:
    """GET /api/flyers/{store_id}/{locale}?date=... — returns store info dict or None.

    A valid store ID with at least one active flyer returns storeName inside
    the first flyer object.  An unknown ID or empty-flyer response returns None.
    """
    try:
        r = requests.get(
            f"{METRO_API_BASE}/flyers/{store_id}/{brand.locale}",
            headers=metro_headers(brand),
            params={"date": date},
            timeout=10,
        )
    except requests.RequestException as exc:
        print(f"  [!] store_id {store_id}: request error – {exc}")
        return None
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    flyers = data.get("flyers", [])
    if not flyers:
        return None
    api_banner = data.get("banner", "")
    if brand.expected_banner and api_banner != brand.expected_banner:
        return None
    return {
        "store_name": flyers[0].get("storeName", ""),
        "banner":     api_banner,
    }


def metro_fetch_store_flyers(brand: MetroBrand, store_id: int, date: str) -> list:
    """Return flyer list for a store on a given date."""
    try:
        r = requests.get(
            f"{METRO_API_BASE}/flyers/{store_id}/{brand.locale}",
            headers=metro_headers(brand),
            params={"date": date},
            timeout=10,
        )
    except requests.RequestException:
        return []
    if r.status_code != 200:
        return []
    try:
        return r.json().get("flyers", [])
    except ValueError:
        return []


def metro_fetch_products(
    brand: MetroBrand, job: str, store_id: int, locale: str | None = None
) -> list:
    """POST /api/Pages/{job}/{store_id}/{locale}/search. Returns flat product list.

    Uses brand.locale by default; pass locale to override.
    """
    locale = locale or brand.locale
    try:
        r = requests.post(
            f"{METRO_API_BASE}/Pages/{job}/{store_id}/{locale}/search",
            headers=metro_headers(brand),
            json={"display_type": "all"},
            timeout=30,
        )
    except requests.RequestException:
        return []
    if r.status_code != 200:
        return []
    try:
        return [p for block in r.json() for p in block.get("products", [])]
    except ValueError:
        return []
