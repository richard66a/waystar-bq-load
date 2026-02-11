# FTP Log Pipeline — Documentation

This folder is the canonical documentation set for the GCP‑native FTP log pipeline.

## Quick Links

- Ops validation runbook: [runbook_ops_validation.md](runbook_ops_validation.md)
- Implementation plan: [IMPLEMENTATION_PLAN.md](IMPLEMENTATION_PLAN.md)
- PR checklist: [PR_CHECKLIST.md](PR_CHECKLIST.md)
- Deployment checklist: [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md)
- Upstream .NET changes: [DOTNET_UPSTREAM_CHANGES.md](DOTNET_UPSTREAM_CHANGES.md)
- Scheduled query & IAM: [SCHEDULED_QUERY_IAM.md](SCHEDULED_QUERY_IAM.md)
- Validation guide: [VALIDATION.md](VALIDATION.md)
- Monitoring runbook (SQL): [../sql/runbook_monitoring.sql](../sql/runbook_monitoring.sql)
- Reprocessing runbook (SQL): [../sql/runbook_reprocessing.sql](../sql/runbook_reprocessing.sql)
- Validation queries: [../sql/validation_queries.sql](../sql/validation_queries.sql)
- ETL procedure: [../sql/06_scheduled_query_proc.sql](../sql/06_scheduled_query_proc.sql)
- ETL script (scheduled): [../sql/06_scheduled_query_etl_scheduled.sql](../sql/06_scheduled_query_etl_scheduled.sql)

## Getting Started

1) Copy the example config:
- [config/example.settings.sh](../config/example.settings.sh) → config/settings.sh

2) Deploy resources:
- [scripts/deploy.sh](../scripts/deploy.sh)

3) Upload test data:
- [tests/generate_test_data.py](../tests/generate_test_data.py)

4) Run ETL:
- [scripts/run_etl.sh](../scripts/run_etl.sh)

5) Validate:
- [sql/validation_queries.sql](../sql/validation_queries.sql)

## What Changed Recently

See [CHANGELOG.md](CHANGELOG.md) for the latest pipeline changes (diagnostics, fingerprinting, dedupe).

## Reference

- Scripts: [../scripts](../scripts)
- SQL: [../sql](../sql)
- Tests: [../tests](../tests)
- Cloud Function (optional): [../cloud_function](../cloud_function)
