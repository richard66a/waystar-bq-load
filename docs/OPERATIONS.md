# Operations

## Running ETL

### Manual Execution

```bash
./scripts/run_etl.sh
```

Or call the stored procedure directly:

```bash
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} \
  "CALL \`${PROJECT_ID}.${DATASET_ID}.proc_process_ftplog\`();"
```

### Scheduled Execution

ETL runs automatically every 5 minutes via Cloud Scheduler or BigQuery Scheduled Query.

Check scheduled query status in BigQuery Console > Scheduled Queries > View Runs.

## Monitoring

### Health Check Query

```bash
bq query --use_legacy_sql=false < sql/runbook_monitoring.sql
```

Key metrics to monitor:
- Files processed per hour
- Pending files count
- Error rate (should be <5%)
- Processing latency (should be <15 min)

### Alerting

Deploy monitoring alerts:

```bash
./scripts/deploy_monitoring_alerts.sh
```

This creates a Cloud Function that checks pipeline health and writes to `pipeline_monitoring` table.

## Reprocessing Files

### Single File

```bash
# Remove from tracking
bq query --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.processed_files\` WHERE gcs_uri = 'gs://bucket/logs/file.json'"

# Remove from base table
bq query --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.base_ftplog\` WHERE gcs_uri = 'gs://bucket/logs/file.json'"

# Re-run ETL
./scripts/run_etl.sh
```

### All Failed Files

```bash
# See failed files
bq query --use_legacy_sql=false \
  "SELECT gcs_uri, status, error_message FROM \`${PROJECT_ID}.${DATASET_ID}.processed_files\` WHERE status != 'SUCCESS'"

# Remove failed from tracking (they'll reprocess)
bq query --use_legacy_sql=false \
  "DELETE FROM \`${PROJECT_ID}.${DATASET_ID}.processed_files\` WHERE status IN ('FAILED', 'PARTIAL')"
```

See `sql/runbook_reprocessing.sql` for more scenarios.

## Troubleshooting

### Files Not Processing

1. Check external table can see files:
```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.external_ftplog_files\`"
```

2. Check for pending files:
```bash
bq query --use_legacy_sql=false \
  "SELECT DISTINCT _FILE_NAME FROM \`${PROJECT_ID}.${DATASET_ID}.external_ftplog_files\`
   WHERE NOT EXISTS (SELECT 1 FROM \`${PROJECT_ID}.${DATASET_ID}.processed_files\` pf WHERE pf.gcs_uri = _FILE_NAME)"
```

3. Verify scheduled query is running in BigQuery Console.

### Duplicate Data

Check for duplicate file processing:
```bash
bq query --use_legacy_sql=false \
  "SELECT gcs_uri, COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.processed_files\` GROUP BY gcs_uri HAVING COUNT(*) > 1"
```

See deduplication queries in `sql/runbook_reprocessing.sql`.

### Parse Errors

Check archive for raw JSON to debug:
```bash
bq query --use_legacy_sql=false \
  "SELECT raw_json FROM \`${PROJECT_ID}.${DATASET_ID}.archive_ftplog\` WHERE originating_filename = 'problem-file' LIMIT 10"
```

### Logs

- **Scheduled Query**: BigQuery Console > Scheduled Queries > View Runs
- **Cloud Function**: `gcloud functions logs read process-ftplog --gen2 --region=us-central1`

## Cleanup

To remove all pipeline resources:

```bash
./scripts/teardown.sh --confirm
```

**Warning**: This permanently deletes all data.
