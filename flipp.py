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

# Placeholder portfolio — fill in when Metro script is built
METRO_PORTFOLIO: list[Brand] = []


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
    """Return list of active publication objects for a store."""
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
    return data if isinstance(data, list) else []


def fetch_publication_products(pub_id: int | str, access_token: str) -> list:
    """Return all products for a publication ID."""
    data = get(
        f"{FLIPP_BASE}/publication/{pub_id}/products",
        params={"display_type": "all", "locale": "en", "access_token": access_token},
    )
    return data if isinstance(data, list) else []


def now_utc() -> datetime:
    return datetime.now(timezone.utc)
