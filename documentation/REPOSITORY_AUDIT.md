# Repository Audit Tracker

Updated: 2026-05-12

## Scope

This tracker records implementation of the repository audit covering:

- unused artifact cleanup
- documentation hardening
- quality guardrail improvements
- no-regression verification for weekly workflow

## Current execution graph (protected paths)

Critical runtime path:

1. `scripts/fetch_flyers.py`
2. `pipeline.clean`
3. `pipeline.build_db --score`
4. `pipeline.flyer_ranker`
5. `scripts/export_frontend_data.py`

These paths are treated as non-negotiable during cleanup.

## Cleanup inventory

| Item | Status | Decision | Evidence |
|---|---|---|---|
| `frontend/src/components/HelloWorld.vue` | Completed | Delete | No references found in `frontend/src/**` |
| `scripts/migrate_to_parquet.py` | Completed | Keep (legacy) | Self-contained one-time migration fallback utility |
| `scripts/_check_flipp.py` | Completed | Delete | Unreferenced ad-hoc probe with hardcoded `/app` paths |
| `scripts/_check_flyers.py` | Completed | Delete | Unreferenced ad-hoc probe with hardcoded `/app` paths |
| `scripts/_check_lat.py` | Completed | Delete | Unreferenced ad-hoc probe with hardcoded `/app` paths |
| `scripts/_check_regions.py` | Completed | Delete | Unreferenced ad-hoc probe with hardcoded `/app` paths |
| `scripts/_gen_mock_deals.py` | Completed | Delete | Unreferenced mock-data generator superseded by pipeline export |
| `scripts/_sanity_check.py` | Completed | Delete | Unreferenced ad-hoc validation probe with hardcoded `/app` paths |

## Documentation hardening

Completed:

- Replaced boilerplate frontend README with project-specific usage/docs.
- Updated root README to describe Parquet outputs and added documentation index.
- Removed credential/token values from `documentation/Stores.md` and replaced with security guidance.
- Add `documentation/PIPELINE.md` with explicit input/output contracts per stage.
- Add `documentation/OPERATIONS.md` runbook for weekly runs and failure recovery.
- Add `documentation/SCHEMA.md` for key Parquet outputs used by backend/frontend.

Planned:

- No remaining documentation gaps in the current audit scope.

## Quality guardrails

Completed:

- Expand CI syntax checks beyond fetch scripts.
- Add coverage reporting in test runs.
- Add `pytest-cov` to developer dependencies.
- Introduced optional non-blocking mypy pass for core packages.
- Add frontend export schema regression tests (`tests/test_export_frontend_data.py`).

Planned next pass:

- No remaining quality-guardrail gaps in the current audit scope.

## Risks and constraints

- Avoid deleting scripts until usage is confirmed through references and operational checks.
- Maintain idempotent rerun behavior for fetch and pipeline commands.
- Preserve compatibility with existing scheduled jobs and frontend data export format.

## Next implementation batch

1. Decide whether to keep `scripts/migrate_to_parquet.py` in place or relocate to a `legacy/` folder.
2. Optionally make mypy CI step blocking after one full clean CI cycle.
3. Add a small CI smoke run for `scripts/export_frontend_data.py --rankings-only`.