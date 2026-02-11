# Scheduled Query & IAM Guidance

This document outlines the steps for creating the scheduled query that calls the stored procedure and for ensuring IAM permissions.

## Scheduled Query

- Create a scheduled query that runs `CALL project.dataset.proc_process_ftplog()` every 5 minutes.
- Use the service account `sa-logviewer-prd` (or environment‑specific equivalent) as the execution identity.
- If the CLI fails to create the scheduled query, use the BigQuery UI and paste the SQL from `sql/06_scheduled_query_call.sql`.

## IAM Roles Required

Grant the scheduled query service account the following roles:

- BigQuery Data Editor (table write access)
- BigQuery Job User (query execution)
- Storage Object Viewer (read from the GCS bucket)

## Verification

- Confirm scheduled query runs are succeeding in the BigQuery UI.
- Confirm new files appear in `processed_files` with `status = SUCCESS`.
