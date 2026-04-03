"""
Pipeline orchestrator — CLI entry point.

Usage::

    python clean.py [--store <name>] [--dry-run]

Options
-------
--store <name>
    Restrict processing to a single store folder (e.g. ``food_basics``).
    Useful for targeted testing without walking every brand directory.
--dry-run
    Print the total record count to stdout and exit 0 without writing any
    output files.  (Full output writing is wired up in milestone P5-1.)
"""

from __future__ import annotations

import argparse
import sys

from load_raw import iter_records


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="clean.py",
        description="Walk raw flyer files, normalise records, and write output.",
    )
    parser.add_argument(
        "--store",
        metavar="NAME",
        default=None,
        help="Process only this store folder (e.g. food_basics).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print record count and exit without writing output.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    count = 0
    for _ in iter_records(store=args.store):
        count += 1

    if args.dry_run:
        print(f"{count} records")
        return 0

    # Full output writing will be implemented in P5-1.
    print(f"{count} records processed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
