# Cloud Function Module

Optional real-time processing alternative to the scheduled query approach.

## Overview

This Cloud Function is triggered when new NDJSON files are uploaded to GCS. It provides near real-time processing (<1 minute latency) compared to the 5-15 minute latency of scheduled queries.

## When to Use

| Approach | Latency | Use Case |
|----------|---------|----------|
| **Scheduled Query** (default) | 5-15 min | Standard batch processing |
| **Cloud Function** (this) | <1 min | Real-time requirements |

Choose the Cloud Function when:
- Near real-time processing is required
- Per-file error handling and retries are needed
- Custom per-file logic that's hard to express in SQL

## Files

| File | Description |
|------|-------------|
| `main.py` | Cloud Function entry points |
| `etl_sql.sql` | SQL for HTTP-triggered scheduled ETL |
| `requirements.txt` | Python dependencies |

## Entry Points

The function exposes three entry points:

1. **`process_ftplog`** - GCS trigger for real-time processing
2. **`run_scheduled_etl`** - HTTP trigger for scheduled ETL via Cloud Scheduler
3. **`run_monitoring_alert`** - HTTP trigger for pipeline health checks

## Deployment

```bash
source ../config/settings.sh

gcloud functions deploy process-ftplog \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=process_ftplog \
    --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
    --trigger-event-filters="bucket=${GCS_BUCKET}" \
    --service-account=${SERVICE_ACCOUNT} \
    --memory=512MB \
    --timeout=300s \
    --set-env-vars="PROJECT_ID=${PROJECT_ID},DATASET_ID=${DATASET_ID}"
```

## Local Testing

```bash
python main.py --bucket $GCS_BUCKET --file logs/test-file.json
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROJECT_ID` | - | GCP project ID |
| `DATASET_ID` | `logviewer` | BigQuery dataset |
| `BASE_TABLE` | `base_ftplog` | Target table for structured data |
| `ARCHIVE_TABLE` | `archive_ftplog` | Raw JSON archive table |
| `PROCESSED_TABLE` | `processed_files` | File tracking table |
| `GCS_LOGS_PREFIX` | `logs` | GCS prefix to watch |
