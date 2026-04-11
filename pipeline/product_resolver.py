"""
pipeline/product_resolver.py — Canonical product identity.

Assigns a stable ``canonical_product_id`` to every flyer observation,
enabling price history tracking and cross-chain comparison.

Matching strategy (three tiers, applied in order)
--------------------------------------------------
Tier 1 — **Strict**: exact ``(store_chain, sku)`` pair.
    ``match_tier="strict"``, ``match_tier_conf=1.0``

Tier 2 — **Probable**: name fingerprint + ≥ 2 of {brand, weight_unit, category_l3}.
    ``match_tier="probable"``, ``match_tier_conf=0.6``

Tier 3 — **Category fallback**: synthetic key from ``category_l3`` (or ``category_l1``).
    ``match_tier="category"``, ``match_tier_conf=0.2``

Name fingerprint algorithm
--------------------------
1. Lowercase ``name_en`` (fall back to ``name_fr``).
2. Strip punctuation and size tokens (e.g. ``500g``, ``1L``, ``2pk``, ``pkg``).
3. Sort remaining tokens alphabetically.
4. SHA-256 hash the result → first 12 hex characters.

Public API
----------
``resolve_products(observations_dir, out_path)`` — see function docstring.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections import Counter
from typing import Any


# ── Name fingerprint ──────────────────────────────────────────────────────────

# Matches a leading number (optional) followed by a size unit, e.g. "500g", "1.5L", "2pk"
_SIZE_TOKEN_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:g|kg|ml|l|lb|oz|pk|pkg|ct|count|pack)\b",
    re.IGNORECASE,
)

# Standalone size/packaging words without a leading number, e.g. "pkg", "pack", "ct"
_STANDALONE_SIZE_RE = re.compile(
    r"\b(?:pkg|pack|ct|count)\b",
    re.IGNORECASE,
)

# Any character that is not a word character or whitespace
_PUNCT_RE = re.compile(r"[^\w\s]")


def _name_fingerprint(name_en: str | None, name_fr: str | None = None) -> str | None:
    """Compute a normalised 12-character SHA-256 fingerprint for a product name.

    Parameters
    ----------
    name_en:
        English product name (preferred).
    name_fr:
        French product name (fallback when ``name_en`` is absent).

    Returns
    -------
    str | None
        First 12 hex characters of the SHA-256 digest, or ``None`` when no
        usable name is available.
    """
    name = name_en or name_fr
    if not name or not name.strip():
        return None

    name = name.lower()
    name = _SIZE_TOKEN_RE.sub(" ", name)
    name = _STANDALONE_SIZE_RE.sub(" ", name)
    name = _PUNCT_RE.sub(" ", name)

    tokens = sorted(name.split())
    if not tokens:
        return None

    joined = " ".join(tokens)
    return hashlib.sha256(joined.encode()).hexdigest()[:12]


# ── Canonical ID helpers ──────────────────────────────────────────────────────


def _canonical_id(*parts: Any) -> str:
    """Return the first 12 hex characters of the SHA-256 hash of *parts*.

    Each part is converted to a string; ``None`` becomes the empty string.
    Parts are joined by a ``|`` separator before hashing.
    """
    key = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha256(key.encode()).hexdigest()[:12]


# ── Single-record resolver ────────────────────────────────────────────────────


def _resolve_record(record: dict) -> tuple[str, str, float]:
    """Return ``(canonical_product_id, match_tier, match_tier_conf)`` for *record*.

    The three tiers are tried in order; the first eligible tier wins.

    Parameters
    ----------
    record:
        A flat dict whose keys correspond to :class:`~pipeline.schema.FlyerItem`
        field names (as produced by ``pyarrow``'s ``Table.to_pylist()``).

    Returns
    -------
    tuple[str, str, float]
        ``(canonical_product_id, match_tier, match_tier_conf)``
    """
    store_chain = record.get("store_chain") or None
    sku = record.get("sku") or None

    # ── Tier 1: strict ────────────────────────────────────────────────────────
    if store_chain and sku:
        cid = _canonical_id("strict", store_chain, sku)
        return cid, "strict", 1.0

    # ── Tier 2: probable ──────────────────────────────────────────────────────
    fingerprint = _name_fingerprint(record.get("name_en"), record.get("name_fr"))
    brand = record.get("brand") or None
    weight_unit = record.get("weight_unit") or None
    category_l3 = record.get("category_l3") or None

    signals = [x for x in (brand, weight_unit, category_l3) if x is not None]
    if fingerprint and len(signals) >= 2:
        cid = _canonical_id("probable", fingerprint, brand, weight_unit, category_l3)
        return cid, "probable", 0.6

    # ── Tier 3: category fallback ────────────────────────────────────────────
    cat_key = record.get("category_l3") or record.get("category_l1") or "unknown"
    cid = _canonical_id("category", cat_key)
    return cid, "category", 0.2


# ── Main API ──────────────────────────────────────────────────────────────────


def resolve_products(observations_dir: str, out_path: str) -> dict[str, str]:
    """Resolve canonical product IDs for all observations.

    Walks every ``.parquet`` file under *observations_dir*, assigns a
    ``canonical_product_id`` to each row via the three-tier matching strategy,
    and writes a ``products.parquet`` dimension table to *out_path*.

    Parameters
    ----------
    observations_dir:
        Root of the observations Parquet tree, e.g. ``"db/observations"``.
    out_path:
        Destination path for the dimension table,
        e.g. ``"db/dimensions/products.parquet"``.

    Returns
    -------
    dict[str, str]
        Mapping of *observation_key* → *canonical_product_id* for every row
        processed.  The observation key is the record's
        ``price_observation_key`` when that field is populated; otherwise a
        deterministic surrogate is synthesised from ``store_chain``,
        ``store_id``, ``flyer_id``, and the row index within the file.

    Side-effects
    ------------
    *   Creates parent directories of *out_path* if they do not exist.
    *   Writes ``products.parquet`` — always overwritten for idempotency.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # ── Collect all observation rows ──────────────────────────────────────────

    all_records: list[dict] = []
    skipped_files: int = 0

    for dirpath, _dirs, filenames in os.walk(observations_dir):
        for fname in sorted(filenames):
            if not fname.endswith(".parquet"):
                continue
            fpath = os.path.join(dirpath, fname)
            try:
                # Use ParquetFile.read() to avoid PyArrow merging Hive
                # partition columns (e.g. store_chain=loblaws in directory
                # name) with same-named columns already inside the file.
                table = pq.ParquetFile(fpath).read()
                all_records.extend(table.to_pylist())
            except Exception as exc:  # noqa: BLE001
                skipped_files += 1
                print(f"Warning: skipping {fpath}: {type(exc).__name__}: {exc}")
                continue

    if skipped_files:
        print(f"resolve_products: {skipped_files} file(s) skipped due to read errors.")

    # ── Assign canonical IDs ──────────────────────────────────────────────────

    obs_to_canonical: dict[str, str] = {}

    # canonical_product_id → accumulated rows
    groups: dict[str, list[dict]] = {}
    # canonical_product_id → tier (set on first encounter; stable per hash)
    group_tiers: dict[str, str] = {}

    for idx, record in enumerate(all_records):
        canonical_product_id, tier, _conf = _resolve_record(record)

        # Determine the observation key
        obs_key: str | None = record.get("price_observation_key") or None
        if not obs_key:
            obs_key = _canonical_id(
                record.get("store_chain"),
                record.get("store_id"),
                record.get("flyer_id"),
                idx,
            )

        obs_to_canonical[obs_key] = canonical_product_id

        if canonical_product_id not in groups:
            groups[canonical_product_id] = []
            group_tiers[canonical_product_id] = tier
        groups[canonical_product_id].append(record)

    # ── Build products dimension table ────────────────────────────────────────

    def _most_common(values: list) -> Any:
        non_null = [v for v in values if v is not None]
        if not non_null:
            return None
        return Counter(non_null).most_common(1)[0][0]

    def _bool_field(values: list) -> bool:
        """Return the most-common boolean among *values*, defaulting to ``False``."""
        val = _most_common(values)
        return bool(val) if val is not None else False

    rows: list[dict] = []
    for canonical_product_id, recs in groups.items():
        tier = group_tiers[canonical_product_id]

        rows.append(
            {
                "canonical_product_id": canonical_product_id,
                "canonical_name": _most_common([r.get("name_en") for r in recs]),
                "canonical_brand": _most_common([r.get("brand") for r in recs]),
                "category_l1": _most_common([r.get("category_l1") for r in recs]),
                "category_l2": _most_common([r.get("category_l2") for r in recs]),
                "category_l3": _most_common([r.get("category_l3") for r in recs]),
                "is_food": _bool_field([r.get("is_food") for r in recs]),
                "is_human_food": _bool_field([r.get("is_human_food") for r in recs]),
                "weight_value": _most_common([r.get("weight_value") for r in recs]),
                "weight_unit": _most_common([r.get("weight_unit") for r in recs]),
                "match_tier": tier,
                "observation_count": len(recs),
            }
        )

    # ── Write products.parquet ────────────────────────────────────────────────

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    schema = pa.schema(
        [
            ("canonical_product_id", pa.string()),
            ("canonical_name", pa.string()),
            ("canonical_brand", pa.string()),
            ("category_l1", pa.string()),
            ("category_l2", pa.string()),
            ("category_l3", pa.string()),
            ("is_food", pa.bool_()),
            ("is_human_food", pa.bool_()),
            ("weight_value", pa.float64()),
            ("weight_unit", pa.string()),
            ("match_tier", pa.string()),
            ("observation_count", pa.int64()),
        ]
    )

    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, out_path)

    return obs_to_canonical
