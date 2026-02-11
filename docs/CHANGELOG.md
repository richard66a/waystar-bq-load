# Changelog

## 2026-02-10
- Added deterministic `hash_fingerprint` support in ETL (SHA256 hex).
- Added per‑file diagnostics in `processed_files` (`rows_expected`, `parse_errors`, `status`).
- Updated validation queries and ops runbook to surface parse errors.
- Hardened scripts (strict mode) and standardized configuration handling.
- Updated test data generator and tests to produce UTC timestamps and deterministic `hash_code` values.
