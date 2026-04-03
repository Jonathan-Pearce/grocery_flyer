"""
Shared utilities for the Flipp Enterprise store/flyer scrapers.

Defines the Brand dataclass, portfolio configs, a FlippLogger, and all
HTTP helper functions so each portfolio script stays thin.
"""

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import requests

FLIPP_BASE = "https://dam.flippenterprise.net/flyerkit"
DELAY = 0.05  # seconds between requests


# ── Brand config ──────────────────────────────────────────────────────────────

@dataclass
class Brand:
    name: str           # Human-readable display name
    slug: str | None    # Flipp URL slug: /flyerkit/store/<slug> and
                        # /flyerkit/publications/<slug>.  None = not yet confirmed.
    folder: str         # Subdirectory name under data/ and logs/
    access_token: str
    code_range: range   # Store-code sweep range used by the store-scanner
    flyer_type_filter: str | None = None  # If set, only publications with this flyer_type are kept


# Slugs confirmed via live URL test; None = slug unknown, brand skipped until confirmed.
LOBLAWS_PORTFOLIO: list[Brand] = [
    Brand("Loblaws",                  "loblaws",  "loblaws",                  "fd66ddd31b95e07b9ad2744424e9fd32", range(1,  3000)),
    Brand("No Frills",                "nofrills", "nofrills",                 "1063f92aaf17b3dfa830cd70a685a52b", range(1, 10001)),
    Brand("Provigo",                  "provigo",  "provigo",                  "31c52dc6a419dc10959261a5a210fccf", range(1, 12001)),
    # Slugs below need confirmation before the store scanner or flyer fetcher can run
    Brand("Real Canadian Superstore", "realcanadiansuperstore", "real_canadian_superstore", "a6e07e290f469d032d54a252f7582de2", range(1, 12001)),
    Brand("Maxi",                     "maxi",     "maxi",                     "75a33b973cc2e856dd0f2cd629d80a19", range(1, 12001)),
    Brand("Zehrs",                    "zehrs",    "zehrs",                    "fef2a837ffeee9e5e5d02f31db81f209", range(1, 12001)),
    Brand("Fortinos",                 "fortinos", "fortinos",                 "ff3274ff57f481a8fcfac9c6c968fe67", range(1, 12001)),
    Brand("Atlantic Superstore",      "atlanticsuperstore", "atlantic_superstore", "4d9c0561f7abbf53ad6eca20dad201c7", range(1, 12001)),
    Brand("Dominion",                 "dominion", "dominion",                 "23d83ed8a192329f29749c3b86c707fc", range(1, 12001)),
    Brand("Independent Grocer",       "yourindependentgrocer", "independent_grocer", "fa31161a375478b68b2ec0f8f8edd65a", range(1, 12001)),
    Brand("Independent City Market",  "independentcitymarket", "independent_city_market", "a30dee18036c0131c522b0fd12632b7d", range(1, 12001)),
    Brand("Freshmart",                 "freshmart",             "freshmart",               "32520249c4e20e14b33e5d45d084cb53", range(1, 12001)),
]

WALMART_PORTFOLIO: list[Brand] = [
    Brand("Walmart", "walmartcanada", "walmart", "92bcff5f7d07c3aaa4b33e2c048d7728", range(1, 10001),
          flyer_type_filter="groceryflyer"),
]

# Slugs confirmed via live URL test; None = slug unknown, brand skipped until confirmed.
SOBEYS_PORTFOLIO: list[Brand] = [
    Brand("Sobeys",   "sobeys",   "sobeys",   "afbc75b4e335236182ac2fba092a0d4a", range(1, 12001)),
    Brand("Safeway",  "safewaycanada", "safeway",  "41073822c1e3a003da36de785443fa0f", range(1, 12001)),
    Brand("IGA",      "igaquebec", "iga",      "692be3f8ba9e9247dc13d064cb89e7f9", range(1, 12001)),
    Brand("Freshco",  "freshco",   "freshco",  "881f0b9feea3693a704952a69b2a037a", range(1, 12001)),
    Brand("Foodland", "foodland",  "foodland", "07ca28af93a0585f05575bf41ce92a6d", range(1, 12001)),
    Brand("Longos",   "longos",    "longos",   "5b4ad9bb0148449f25dbb0b76b976c1b", range(1, 12001)),
    Brand("Farm Boy", "farmboy",   "farm_boy", "633f9e9fe2eae3e7b4a811dd9690ac4b", range(1, 12001)),
]

# ── Metro portfolio ──────────────────────────────────────────────────────

METRO_API_BASE = "https://metrodigital-apim.azure-api.net/api"


@dataclass
class MetroBrand:
    name: str
    folder: str
    app_config_url: str  # SPA app.json URL; used to fetch banner_id/api_key if None
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


# ── Logging ───────────────────────────────────────────────────────────────────

class FlippLogger:
    """Writes every message to stdout and to a dated verbose log file.
    Summary messages also go to a separate dated summary log file."""

    def __init__(self, log_dir: str, today: str):
        os.makedirs(log_dir, exist_ok=True)
        self.verbose_path = os.path.join(log_dir, f"{today}_verbose.log")
        self.summary_path = os.path.join(log_dir, f"{today}_summary.log")
        self._v = open(self.verbose_path, "a")
        self._s = open(self.summary_path, "a")

    def log(self, msg: str = "") -> None:
        """Verbose + stdout."""
        print(msg)
        self._v.write(msg + "\n")
        self._v.flush()

    def summary(self, msg: str = "") -> None:
        """Verbose + summary + stdout."""
        self.log(msg)
        self._s.write(msg + "\n")
        self._s.flush()

    def close(self) -> None:
        self._v.close()
        self._s.close()


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def get(url: str, params: dict) -> list | dict | None:
    """GET request returning parsed JSON, or None on any error."""
    try:
        resp = requests.get(url, params=params, timeout=10)
    except requests.RequestException as exc:
        print(f"    [!] request error – {exc}")
        return None
    if resp.status_code != 200:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def save_json(path: str, data, log_fn=print) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    log_fn(f"  Saved → {path}  ({os.path.getsize(path):,} bytes)")


# ── Store-level fetchers ──────────────────────────────────────────────────────

def fetch_store(brand: Brand, code: int) -> dict | None:
    """Return store JSON for a valid store code, or None if not found."""
    try:
        resp = requests.get(
            f"{FLIPP_BASE}/store/{brand.slug}",
            params={"locale": "en", "access_token": brand.access_token, "store_code": code},
            timeout=10,
        )
    except requests.RequestException as exc:
        print(f"  [!] code {code}: request error – {exc}")
        return None
    if resp.status_code != 200:
        return None
    try:
        data = resp.json()
    except ValueError:
        return None
    return data if data else None


def fetch_store_publications(brand: Brand, store_code: str) -> list:
    """Return list of active publication objects for a store.

    If brand.flyer_type_filter is set, only publications whose flyer_type
    matches that value are returned.
    """
    data = get(
        f"{FLIPP_BASE}/publications/{brand.slug}",
        params={
            "languages[]": "en",
            "locale": "en",
            "access_token": brand.access_token,
            "store_code": store_code,
        },
    )
    if data is None:
        return []
    if isinstance(data, dict):
        data = data.get("flyers", data.get("publications", []))
    if not isinstance(data, list):
        return []
    if brand.flyer_type_filter:
        data = [p for p in data if p.get("flyer_type") == brand.flyer_type_filter]
    return data


def fetch_publication_products(pub_id: int | str, access_token: str) -> list:
    """Return all products for a publication ID."""
    data = get(
        f"{FLIPP_BASE}/publication/{pub_id}/products",
        params={"display_type": "all", "locale": "en", "access_token": access_token},
    )
    return data if isinstance(data, list) else []


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ── Metro HTTP helpers ────────────────────────────────────────────────────────

def metro_load_credentials(brand: "MetroBrand") -> bool:
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


def metro_headers(brand: "MetroBrand") -> dict:
    return {
        "Ocp-Apim-Subscription-Key": brand.api_key,
        "Banner": brand.banner_id,
        "User-Agent": "Mozilla/5.0",
    }


def metro_fetch_store(brand: "MetroBrand", store_id: int, date: str) -> dict | None:
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


def metro_fetch_store_flyers(brand: "MetroBrand", store_id: int, date: str) -> list:
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
    brand: "MetroBrand", job: str, store_id: int, locale: str | None = None
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
