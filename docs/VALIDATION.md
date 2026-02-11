# Validation Guide

Use `sql/validation_queries.sql` for comprehensive checks. The most important checks are summarized here.

## Required Checks

1) **Processed files status**
- `processed_files.status` should be `SUCCESS` for new files.
- `rows_expected == rows_loaded` and `parse_errors == 0` for new files.

2) **Row parity**
- Counts in `base_ftplog` and `archive_ftplog` should match for the new file.

3) **Duplicate detection**
- Duplicate groups by `hash_fingerprint` should be zero for newly ingested files.

## Where to Look

- `sql/validation_queries.sql`
- `sql/runbook_monitoring.sql`
- `docs/runbook_ops_validation.md`
