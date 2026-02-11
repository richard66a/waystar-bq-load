# Deployment Checklist

## Preflight
- [ ] Update `config/settings.sh` for the target environment.
- [ ] Confirm GCP project, dataset, and bucket names.
- [ ] Confirm service account IAM roles:
  - BigQuery Data Editor
  - BigQuery Job User
  - Storage Object Viewer
- [ ] Ensure required CLIs are installed: `gcloud`, `bq`, `gsutil`, `jq`.

## Deploy
- [ ] Deploy infrastructure using `scripts/deploy.sh`.
- [ ] Create scheduled query using `scripts/create_scheduled_query.sh` (or UI).
- [ ] (Optional) Deploy Cloud Function path if real-time is required.

## Validate
- [ ] Upload a sample NDJSON file to `gs://<bucket>/logs/`.
- [ ] Run ETL (manual or scheduled) and verify `processed_files` status.
- [ ] Run validation queries in `sql/validation_queries.sql`.

## Rollback
- [ ] If needed, remove scheduled queries and delete affected rows.
- [ ] Use `sql/runbook_reprocessing.sql` for targeted reprocessing.
