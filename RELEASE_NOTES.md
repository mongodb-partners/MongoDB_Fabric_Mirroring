# Release notes

All notable changes to this project are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).  
Version numbers follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html): **MAJOR.MINOR.PATCH**.

The canonical application version is the single line in the repo root file **`VERSION`** (also exposed at runtime as `constants.APP_VERSION`).

---

## Git version tags

To record a release in Git after updating `VERSION` and this document:

```bash
git add VERSION RELEASE_NOTES.md
git commit -m "Release 1.3.0"
git tag -a v1.3.0 -m "MongoDB Fabric Mirroring 1.3.0"
git push origin main --follow-tags
```

Adjust branch name as needed. Existing lightweight tags in this repo include `v1.1` and `v1.2`; new releases should use **annotated** tags (`-a`) when possible for traceability.

---

## [Unreleased]

### Planned

- _(Add items here before cutting the next version.)_

---

## [1.4.3] — 2026-07-14

### Changed

- **Init sync** no longer uses a snapshot/`atClusterTime` session. It still captures cluster time **N** at start (persisted as `_init_cluster_time.pkl`) and runs reads on a normal client session; the change stream still opens at **`startAtOperationTime: N+1`** when no resume token exists.

### Fixed

- **`schema_utils.py`**: replace leftover `print` of `conversion_flag` with `logger.debug`.

---

## [1.4.2] — 2026-07-13

### Added

- **Cluster-time handoff** between initial sync and change streams:
  - Capture MongoDB `operationTime` **N** at init start and persist as `_init_cluster_time.pkl`.
  - Init sync reads use a **snapshot** session pinned to **`atClusterTime: N`**.
  - Change stream opens with **`startAtOperationTime: N+1`** when no resume token exists.
- Helpers in **`mongo_cluster_time.py`** (`get_cluster_time`, `next_timestamp`, `start_snapshot_session_at`).

### Fixed

- Documents written after the init snapshot but before the listening thread started were previously missed when the change stream opened from the latest oplog position.

---

## [1.4.1] — 2026-07-07

### Changed

- **`_partnerEvents.json`** is written once at the **landing zone root** (mirrored database level), not per collection folder.
- Partner events template no longer includes a **`MongoDBCollection`** field.

### Fixed

- **`push_file_to_lz.py`**: added **`_lz_folder_url`**, **`get_file_from_lz_root`**, and **`push_file_to_lz_root`** so root-level LZ files use a correct URL path.

---

## [1.4.0] — 2026-06-17

### Added

- **`SCHEMA_BOOTSTRAP_SAMPLE_MAX_ATTEMPTS`** (`4`): `$sample` retries with decreasing size (100%, 75%, 50%, 25% of initial) before falling back to `find().sort(_id).limit(N)`.
- **`_fetch_bootstrap_sample_documents`** and **`_build_schema_from_documents`** in `schema_utils.py` for resilient schema bootstrap.

### Changed

- **Startup order**: `init_sync` runs **synchronously** in `mirror()` before the listening thread starts for each collection.
- **Failed init sync**: change-stream listening is **not** started for that collection; mirroring continues with the next collection in the list.
- Schema bootstrap errors are logged; init sync may still run if bootstrap used the find fallback or partial schema.

### Fixed

- **`listening.py`**: `pymongo.Error` replaced with **`PyMongoError`** (valid base exception in PyMongo 4.x).
- **`$sample` sporadic failures** (e.g. “could not find a non-duplicate document after 100”) no longer crash schema bootstrap or block other collections.

---

## [1.3.0] — 2026-06-17

### Added

- **`VERSION`** file and **`APP_VERSION`** in `constants.py` (read at import time).
- **`RELEASE_NOTES.md`** (this file) for change tracking.
- **Schema bootstrap** via MongoDB aggregation [`$sample`](https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/): random documents for internal schema inference.
- **`SCHEMA_BOOTSTRAP_MAX_FRACTION`** (`0.049`): default sample size is below **5%** of `estimated_document_count`, aligned with `$sample` behavior in the manual.
- Optional **`SCHEMA_BOOTSTRAP_SAMPLE_SIZE`** environment variable: fixed document count for bootstrap sampling (clamped to collection size).

### Changed

- **Parquet / Fabric schema consistency**: `finalize_dataframe_for_parquet` enforces declared schema dtypes before write; object columns without schema still stringify; typed columns use coercion instead of blanket string conversion (reduces Fabric `SchemaMergeFailure` from mixed column types).
- **`init_column_schema`**: null-first values infer type from pandas dtype where possible instead of always defaulting to string.
- **`process_dataframe`**: applies `convert_dtypes` in place on the caller’s DataFrame so conversions are not lost.
- **`_get_first_item`**: uses `first_valid_index()` correctly (no longer treats index `0` as “first valid” when it is null).
- **`init_table_schema`**: bootstrap uses **`$sample`** instead of sequential `find().sort({"_id": 1}).limit(...)`.
- **`.env_example`**, **`README.md`**: documented schema bootstrap and troubleshooting notes.

### Fixed

- **`process_dataframe`** no longer rebinds `df` after `convert_dtypes`, which previously discarded in-memory updates on the init/listening paths.
- **Fabric / batch-size-1 edge cases**: stricter int/bool/float matching, overflow handling, `Decimal128` / `pd.NA` handling, and typed null fallbacks during finalize (Case01581383).

### Removed

- **`schemas`**: `init_column_renaming_to_mem`, `reset_for_tests` (test-only helpers).
- **`schema_utils`**: `bootstrap_table_schema_from_documents` (test-only).
- **`tests/`** and **`requirements-dev.txt`** (pytest scaffolding), per product simplification.

---

## [1.2.0] — _(git tag `v1.2`)_

Summary from repository history (not necessarily exhaustive):

- Change stream resilience: non-resumable errors clear resume token; reconnect behavior.
- PyMongo / connection timeout and error-handling improvements.
- Telemetry: `_partnerEvents.json` support.
- Miscellaneous README and packaging updates.

---

## [1.1.0] — _(git tag `v1.1`)_

Earlier tagged baseline; see `git log` for full history.
