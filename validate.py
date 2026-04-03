"""
QA validation report for cleaned grocery flyer data.

Reads all per-flyer JSON files from the cleaned output directory, loads each
:class:`~schema.FlyerItem` record, and prints a structured five-section QA
report covering record counts, price quality, weight quality, category
coverage, and multi-product statistics.

Usage::

    python validate.py [--input-dir cleaned/] [--json]

Options
-------
--input-dir PATH
    Root directory containing cleaned per-flyer JSON files (default: ``cleaned``).
--json
    Write a machine-readable report to ``<input-dir>/validation_report.json``
    in addition to printing the formatted table to stdout.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from typing import Any

from parsers.weight_parser import parse_weight
from schema import FlyerItem

#: Pounds-to-kilograms conversion factor.
_LB_TO_KG: float = 2.20462

#: Acceptable relative tolerance for lb↔kg cross-validation (2 %).
_KG_LB_TOLERANCE: float = 0.02


# ── Data loader ───────────────────────────────────────────────────────────────


def _load_records(input_dir: str) -> tuple[list[FlyerItem], int]:
    """Walk *input_dir* and return all :class:`~schema.FlyerItem` records.

    Returns
    -------
    tuple[list[FlyerItem], int]
        Loaded records and the total number of JSON files read.
    """
    records: list[FlyerItem] = []
    file_count = 0

    if not os.path.isdir(input_dir):
        return records, file_count

    for entry in sorted(os.scandir(input_dir), key=lambda e: e.name):
        if not entry.is_dir():
            continue
        for file_entry in sorted(os.scandir(entry.path), key=lambda e: e.name):
            if not file_entry.name.endswith(".json"):
                continue
            file_count += 1
            try:
                with open(file_entry.path, encoding="utf-8") as fh:
                    payload = json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
            for raw_record in payload.get("records", []):
                try:
                    records.append(FlyerItem.model_validate(raw_record))
                except Exception:  # noqa: BLE001
                    continue

    return records, file_count


# ── Weight-warning helper (shared by sections 1 and 3) ───────────────────────


def _weight_warning_count(records: list[FlyerItem]) -> int:
    """Return the number of records that trigger weight sanity-check warnings."""
    return sum(
        1
        for r in records
        if parse_weight(r.raw_name, r.raw_description, r.raw_body).get(
            "parse_warnings"
        )
    )


# ── Section builders ──────────────────────────────────────────────────────────


def _section1(records: list[FlyerItem]) -> dict[str, Any]:
    """Section 1 — Record counts."""
    by_store: Counter[str] = Counter(r.store_chain or "unknown" for r in records)
    parents = sum(1 for r in records if r.is_multi_product)
    children = sum(1 for r in records if r.parent_record_id is not None)

    # Re-run the weight parser on preserved raw fields to surface parse_warnings.
    # Banner/inblock records are excluded before the pipeline writes output;
    # their count is not tracked in the cleaned files.
    warn_count = 0
    warning_set: set[str] = set()
    for r in records:
        ws = parse_weight(r.raw_name, r.raw_description, r.raw_body).get(
            "parse_warnings", []
        )
        if ws:
            warn_count += 1
            warning_set.update(ws)

    return {
        "total_records": len(records),
        "by_store": dict(sorted(by_store.items())),
        # Banner/inblock records are excluded before the pipeline writes output.
        "filtered_records": 0,
        "filtered_pct": 0.0,
        "multi_product_parents": parents,
        "multi_product_children": children,
        "records_with_parse_warnings": warn_count,
        "parse_warning_messages": sorted(warning_set),
    }


def _section2(records: list[FlyerItem]) -> dict[str, Any]:
    """Section 2 — Price quality."""
    total = len(records)

    no_sale = [r for r in records if r.sale_price is None]
    # "Expected" absence: multi-buy deals where price is expressed as a total.
    no_sale_expected = sum(1 for r in no_sale if r.multi_buy_total is not None)
    no_sale_unexpected = len(no_sale) - no_sale_expected

    # Anomaly: advertised price higher than the listed regular price.
    anomaly_keys: list[str | None] = [
        r.price_observation_key
        for r in records
        if r.sale_price is not None
        and r.regular_price is not None
        and r.sale_price > r.regular_price
    ]

    # lb ↔ kg cross-validation: price_per_kg should equal price_per_lb × 2.20462
    # within the configured tolerance.
    lb_kg_failure_keys: list[str | None] = []
    for r in records:
        if (
            r.price_per_kg is not None
            and r.price_per_lb is not None
            and r.price_per_lb > 0
        ):
            expected_kg = r.price_per_lb * _LB_TO_KG
            if abs(r.price_per_kg - expected_kg) / expected_kg > _KG_LB_TOLERANCE:
                lb_kg_failure_keys.append(r.price_observation_key)

    return {
        "total_records": total,
        "no_sale_price": len(no_sale),
        "no_sale_price_pct": round(len(no_sale) / total * 100, 1) if total else 0.0,
        "no_sale_expected": no_sale_expected,
        "no_sale_unexpected": no_sale_unexpected,
        "sale_gt_regular_anomalies": len(anomaly_keys),
        "sale_gt_regular_keys": anomaly_keys,
        "lb_kg_cross_validation_failures": len(lb_kg_failure_keys),
        "lb_kg_failure_keys": lb_kg_failure_keys,
        "price_is_floor_count": sum(1 for r in records if r.price_is_floor),
        "multi_buy_qty_count": sum(1 for r in records if r.multi_buy_qty is not None),
    }


def _section3(records: list[FlyerItem]) -> dict[str, Any]:
    """Section 3 — Weight quality."""
    total = len(records)
    sanity_count = _weight_warning_count(records)
    no_weight = sum(
        1 for r in records if r.weight_value is None and r.pack_count is None
    )
    unit_dist: Counter[str] = Counter(
        r.weight_unit for r in records if r.weight_unit is not None
    )

    return {
        "total_records": total,
        "sanity_warning_count": sanity_count,
        "no_weight_extracted": no_weight,
        "no_weight_pct": round(no_weight / total * 100, 1) if total else 0.0,
        "weight_unit_distribution": dict(unit_dist.most_common()),
    }


def _section4(records: list[FlyerItem]) -> dict[str, Any]:
    """Section 4 — Category coverage."""
    total = len(records)
    with_cat = sum(1 for r in records if r.category_l1 is not None)
    food_records = [r for r in records if r.is_human_food]
    food_with_cat = sum(1 for r in food_records if r.category_l1 is not None)

    # Unmapped = category resolved to the catch-all "Other" value.
    unmapped_raw: Counter[str] = Counter()
    for r in records:
        if r.category_l1 == "Other":
            for c in r.raw_categories or []:
                unmapped_raw[c] += 1

    return {
        "total_records": total,
        "with_category_l1": with_cat,
        "category_l1_pct": round(with_cat / total * 100, 1) if total else 0.0,
        "food_total": len(food_records),
        "food_with_category_l1": food_with_cat,
        "food_category_l1_pct": (
            round(food_with_cat / len(food_records) * 100, 1) if food_records else 0.0
        ),
        "unmapped_to_other": sum(1 for r in records if r.category_l1 == "Other"),
        "unmapped_raw_category_strings": dict(unmapped_raw.most_common(20)),
    }


def _section5(records: list[FlyerItem]) -> dict[str, Any]:
    """Section 5 — Multi-product."""
    return {
        "source_multi_product_records": sum(1 for r in records if r.is_multi_product),
        "child_records_generated": sum(
            1 for r in records if r.parent_record_id is not None
        ),
    }


# ── Report builder ────────────────────────────────────────────────────────────


def build_report(input_dir: str) -> dict[str, Any]:
    """Load all records from *input_dir* and return the full QA report dict.

    Parameters
    ----------
    input_dir:
        Root directory containing cleaned per-flyer JSON files.

    Returns
    -------
    dict[str, Any]
        Structured report with keys ``section1_record_counts`` through
        ``section5_multi_product``, plus top-level metadata.
    """
    records, file_count = _load_records(input_dir)
    return {
        "input_dir": input_dir,
        "total_files_read": file_count,
        "section1_record_counts": _section1(records),
        "section2_price_quality": _section2(records),
        "section3_weight_quality": _section3(records),
        "section4_category_coverage": _section4(records),
        "section5_multi_product": _section5(records),
    }


# ── Text formatter ────────────────────────────────────────────────────────────

_COL_WIDTH = 44


def _fmt_table(report: dict[str, Any]) -> str:
    """Render *report* as a human-readable plain-text table."""
    lines: list[str] = []

    def _hdr(title: str) -> None:
        lines.append("")
        lines.append("─" * 62)
        lines.append(f"  {title}")
        lines.append("─" * 62)

    def _row(label: str, value: object) -> None:
        lines.append(f"  {label:<{_COL_WIDTH}} {value}")

    def _list_rows(
        label: str, items: list[Any], limit: int = 10
    ) -> None:
        if not items:
            _row(label, "(none)")
            return
        shown = items[:limit]
        _row(label, shown[0])
        for item in shown[1:]:
            lines.append(f"  {'':{_COL_WIDTH + 1}}{item}")
        if len(items) > limit:
            lines.append(f"  {'':{_COL_WIDTH + 1}}… +{len(items) - limit} more")

    s1 = report["section1_record_counts"]
    s2 = report["section2_price_quality"]
    s3 = report["section3_weight_quality"]
    s4 = report["section4_category_coverage"]
    s5 = report["section5_multi_product"]

    lines.append("=" * 62)
    lines.append("  GROCERY FLYER QA REPORT")
    lines.append(f"  Input: {report['input_dir']}  ({report['total_files_read']} files)")
    lines.append("=" * 62)

    # ── Section 1 ─────────────────────────────────────────────────────────────
    _hdr("1. Record Counts")
    _row("Total records", s1["total_records"])
    for store, count in s1["by_store"].items():
        _row(f"  {store}", count)
    _row(
        "Filtered (banners/inblocks)",
        f"{s1['filtered_records']}  ({s1['filtered_pct']:.1f}%)",
    )
    _row("Multi-product parents", s1["multi_product_parents"])
    _row("Multi-product children", s1["multi_product_children"])
    _row("Records with parse warnings", s1["records_with_parse_warnings"])
    if s1["parse_warning_messages"]:
        _list_rows("  Warning messages", s1["parse_warning_messages"])

    # ── Section 2 ─────────────────────────────────────────────────────────────
    _hdr("2. Price Quality")
    _row("Total records", s2["total_records"])
    _row(
        "  sale_price = None",
        f"{s2['no_sale_price']}  ({s2['no_sale_price_pct']:.1f}%)",
    )
    _row("    expected (multi_buy_total set)", s2["no_sale_expected"])
    _row("    unexpected", s2["no_sale_unexpected"])
    _row("  sale_price > regular_price", s2["sale_gt_regular_anomalies"])
    if s2["sale_gt_regular_keys"]:
        _list_rows(
            "    affected keys",
            [k for k in s2["sale_gt_regular_keys"] if k],
        )
    _row(
        "  lb↔kg cross-validation failures",
        s2["lb_kg_cross_validation_failures"],
    )
    if s2["lb_kg_failure_keys"]:
        _list_rows(
            "    affected keys",
            [k for k in s2["lb_kg_failure_keys"] if k],
        )
    _row("  price_is_floor = True", s2["price_is_floor_count"])
    _row("  multi_buy_qty set", s2["multi_buy_qty_count"])

    # ── Section 3 ─────────────────────────────────────────────────────────────
    _hdr("3. Weight Quality")
    _row("Total records", s3["total_records"])
    _row("Sanity-check warnings", s3["sanity_warning_count"])
    _row(
        "No weight extracted",
        f"{s3['no_weight_extracted']}  ({s3['no_weight_pct']:.1f}%)",
    )
    if s3["weight_unit_distribution"]:
        _row("weight_unit distribution", "")
        for unit, count in s3["weight_unit_distribution"].items():
            _row(f"  {unit}", count)

    # ── Section 4 ─────────────────────────────────────────────────────────────
    _hdr("4. Category Coverage")
    _row("Total records", s4["total_records"])
    _row(
        "With category_l1",
        f"{s4['with_category_l1']}  ({s4['category_l1_pct']:.1f}%)",
    )
    _row(
        "Food records (human) with category_l1",
        f"{s4['food_with_category_l1']} / {s4['food_total']}"
        f"  ({s4['food_category_l1_pct']:.1f}%)",
    )
    _row("Unmapped → 'Other'", s4["unmapped_to_other"])
    if s4["unmapped_raw_category_strings"]:
        _list_rows(
            "  Top unmapped raw categories",
            [
                f"{cat!r}: {cnt}"
                for cat, cnt in s4["unmapped_raw_category_strings"].items()
            ],
        )

    # ── Section 5 ─────────────────────────────────────────────────────────────
    _hdr("5. Multi-Product")
    _row("Source multi-product records", s5["source_multi_product_records"])
    _row("Child records generated", s5["child_records_generated"])

    lines.append("")
    lines.append("=" * 62)
    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="validate.py",
        description="Generate a QA report from cleaned grocery flyer output.",
    )
    parser.add_argument(
        "--input-dir",
        metavar="PATH",
        default="cleaned",
        help="Root directory of cleaned per-flyer JSON files (default: cleaned).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Write machine-readable report to <input-dir>/validation_report.json.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for the QA report.

    Returns
    -------
    int
        Exit code: 0 on success, 1 if the input directory does not exist.
    """
    args = _build_parser().parse_args(argv)
    input_dir: str = args.input_dir

    if not os.path.isdir(input_dir):
        print(
            f"validate.py: input directory not found: {input_dir!r}",
            file=sys.stderr,
        )
        return 1

    report = build_report(input_dir)
    print(_fmt_table(report))

    if args.json:
        out_path = os.path.join(input_dir, "validation_report.json")
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nReport written to {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
