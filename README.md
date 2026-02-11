# FTP Log Pipeline - GCP Native BigQuery Implementation

A production-ready, GCP-native data pipeline for processing FTP log events from GCS to BigQuery.

## Canonical Documentation

For the consolidated docs set, see:

- [docs/README.md](docs/README.md)
- [docs/runbook_ops_validation.md](docs/runbook_ops_validation.md)
- [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md)
- [docs/CHANGELOG.md](docs/CHANGELOG.md)

Configuration template:

- [config/example.settings.sh](config/example.settings.sh)

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Deployment](#deployment)
- [Testing](#testing)
- [Operations](#operations)
- [Troubleshooting](#troubleshooting)
- [Configuration Reference](#configuration-reference)

---

## Overview

This pipeline processes FTP log events stored as NDJSON files in Google Cloud Storage and loads them into BigQuery for analysis.

### Key Features

- **GCP-Native**: Pure SQL-based ETL using BigQuery scheduled queries
- **Idempotent**: Files are tracked to prevent duplicate processing
- **Archive-First**: Raw JSON preserved for compliance and recovery
- **Scalable**: Handles 100-1000 files/day with minimal configuration
- **Observable**: Built-in monitoring queries and health checks

### Pipeline Flow

```
.NET Service → GCS Bucket → BigQuery ETL → Structured Tables
                  │                              │
                  │    ┌────────────────┐       │
                  └───→│ External Table │───────┘
                       └────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        base_ftplog    archive_ftplog   processed_files
        (Structured)   (Raw JSON)       (Tracking)
```

---

## Architecture

### Implementation Options

| Option | Latency | When to Use |
|--------|---------|-------------|
| **Scheduled Query** (Recommended) | 5-15 min | Standard use case |
| **Cloud Function** (Alternative) | <1 min | Real-time requirements |

### BigQuery Tables

| Table | Purpose |
|-------|---------|
| `base_ftplog` | Structured, queryable FTP events (partitioned by `event_dt`) |
| `archive_ftplog` | Raw JSON archive for compliance (partitioned by `archived_timestamp`) |
| `processed_files` | Tracks which GCS files have been processed |
| `external_ftplog_files` | External table pointing to GCS (no data storage) |

---

## Quick Start

### Prerequisites

- Google Cloud SDK (`gcloud`, `bq`, `gsutil`) installed
- GCP project with BigQuery and Cloud Storage APIs enabled
- Appropriate IAM permissions

### 1. Clone and Configure

```bash
cd ftplog-pipeline

# Edit configuration for your environment
vim config/settings.sh
```

### 2. Deploy Infrastructure

```bash
chmod +x scripts/*.sh
./scripts/deploy.sh
```

### 3. Generate Test Data

```bash
python tests/generate_test_data.py --output-dir ./test_files --num-files 3

# Upload to GCS
gsutil cp ./test_files/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/
```

### 4. Run ETL

```bash
# Manual execution
./scripts/run_etl.sh

# Or run SQL directly
bq query --use_legacy_sql=false < sql/06_scheduled_query_etl.sql
```

### 5. Validate

```bash
bq query --use_legacy_sql=false < sql/validation_queries.sql
```

---

## Project Structure

```
ftplog-pipeline/
├── config/
│   └── settings.sh              # Environment configuration
├── sql/
│   ├── 00_setup_all.sql         # Combined setup script
│   ├── 01_create_dataset.sql    # Dataset creation
│   ├── 02_create_base_table.sql # Base table schema
│   ├── 03_create_archive_table.sql # Archive table schema
│   ├── 04_create_processed_table.sql # Tracking table schema
│   ├── 05_create_external_table.sql # External table
│   ├── 06_scheduled_query_etl.sql # Main ETL query
│   ├── 06_scheduled_query_etl_scheduled.sql # DML-only scheduled ETL
│   ├── 06_scheduled_query_call.sql # Scheduled query CALL body
│   ├── 06_scheduled_query_proc.sql # Stored procedure (optional)
│   ├── 07_create_monitoring_table.sql # Monitoring snapshot table
│   ├── 07_pipeline_monitoring_insert.sql # Monitoring snapshot insert
│   ├── validation_queries.sql   # Data validation
│   ├── runbook_reprocessing.sql # Reprocessing scenarios
│   └── runbook_monitoring.sql   # Monitoring queries
├── scripts/
│   ├── deploy.sh                # Full deployment
│   ├── run_etl.sh              # Manual ETL execution
│   ├── teardown.sh             # Cleanup resources
│   └── deploy_cloud_function.sh # Alternative deployment
│   ├── deploy_scheduled_etl.sh  # Cloud Scheduler + HTTP function
│   ├── create_scheduled_query.sh # Scheduled query creator
│   ├── create_monitoring_scheduled_query.sh # Monitoring scheduled query
│   └── deploy_monitoring_alerts.sh # Monitoring alerting deployment
├── cloud_function/
│   ├── main.py                 # Cloud Function code
│   ├── etl_sql.sql              # ETL script for HTTP function
│   └── requirements.txt        # Python dependencies
├── tests/
│   ├── generate_test_data.py   # Test data generator
│   └── sample_data/            # Sample NDJSON files
└── README.md
```

---

## Deployment

### Option A: Scheduled Query (BigQuery Transfer)

```bash
# Deploy all infrastructure
./scripts/deploy.sh

# Preferred: create scheduled query that CALLs the stored procedure
./scripts/create_scheduled_query.sh --call-proc --apply

# If scheduled-query creation rejects the CALL payload, use Option C below.
```

### Option B: Cloud Function (Real-time)

```bash
# Deploy infrastructure first
./scripts/deploy.sh

# Deploy Cloud Function
./scripts/deploy_cloud_function.sh

### Option C: Cloud Scheduler + HTTP Function (Scheduled ETL)

This option runs the multi-statement ETL every 5 minutes via Cloud Scheduler.

```bash
./scripts/deploy_scheduled_etl.sh
```
```

### IAM Requirements

The service account needs:

| Role | Purpose |
|------|---------|
| `roles/bigquery.dataEditor` | Write to BigQuery tables |
| `roles/bigquery.jobUser` | Run BigQuery jobs |
| `roles/storage.objectViewer` | Read from GCS bucket |

---

## Testing

### Generate Test Data

```bash
# Basic test files
python tests/generate_test_data.py -o ./test_files -n 5 -r 100

# With malformed JSON for error testing
python tests/generate_test_data.py -o ./test_files -n 3 -r 50 --include-malformed

# Reproducible output
python tests/generate_test_data.py -o ./test_files -n 3 --seed 42
```

### Upload Test Data

```bash
gsutil cp ./test_files/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/
```

### Validate Processing

```bash
# Check processing status
bq query --use_legacy_sql=false "
SELECT gcs_uri, rows_loaded, status, processed_timestamp
FROM \`sbox-ravelar-001-20250926.logviewer.processed_files\`
ORDER BY processed_timestamp DESC
LIMIT 10"

# Check base table
bq query --use_legacy_sql=false "
SELECT COUNT(*) as rows, COUNT(DISTINCT gcs_uri) as files
FROM \`sbox-ravelar-001-20250926.logviewer.base_ftplog\`"
```

---

## Operations

### Monitoring

```bash
# Pipeline health dashboard (ad-hoc)
bq query --use_legacy_sql=false < sql/runbook_monitoring.sql

# Scheduled monitoring snapshots (writes to logviewer.pipeline_monitoring)
./scripts/create_monitoring_scheduled_query.sh --apply

# Deploy monitoring alerting (Cloud Function + Scheduler + Cloud Monitoring policy)
./scripts/deploy_monitoring_alerts.sh

# Latest health snapshot
bq query --use_legacy_sql=false "
SELECT *
FROM `sbox-ravelar-001-20250926.logviewer.pipeline_monitoring`
ORDER BY check_time DESC
LIMIT 5"
```

Key metrics to monitor:
- Files processed per hour
- Pending files count
- Error rate (should be <5%)
- Processing latency

### Reprocessing Files

```bash
# View reprocessing scenarios
cat sql/runbook_reprocessing.sql

# Reprocess a single file
bq query --use_legacy_sql=false "
DELETE FROM \`sbox-ravelar-001-20250926.logviewer.processed_files\`
WHERE gcs_uri = 'gs://bucket/logs/file-to-reprocess.json';

DELETE FROM \`sbox-ravelar-001-20250926.logviewer.base_ftplog\`
WHERE gcs_uri = 'gs://bucket/logs/file-to-reprocess.json';
"

# Then run ETL
./scripts/run_etl.sh
```

### Manual ETL Execution

```bash
# Run ETL outside of scheduled execution
./scripts/run_etl.sh

# Or directly
bq query --use_legacy_sql=false < sql/06_scheduled_query_etl.sql
```

---

## Troubleshooting

### Common Issues

#### Files Not Processing

1. Check external table can see files:
```sql
SELECT COUNT(*) FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
```

2. Check for pending files:
```sql
SELECT DISTINCT _FILE_NAME
FROM `sbox-ravelar-001-20250926.logviewer.external_ftplog_files`
WHERE NOT EXISTS (
    SELECT 1 FROM `sbox-ravelar-001-20250926.logviewer.processed_files` pf
    WHERE pf.gcs_uri = _FILE_NAME
)
```

3. Verify scheduled query is running (check BigQuery > Scheduled Queries)

#### Duplicate Data

1. Check for duplicate file processing:
```sql
SELECT gcs_uri, COUNT(*) 
FROM `sbox-ravelar-001-20250926.logviewer.processed_files`
GROUP BY gcs_uri HAVING COUNT(*) > 1
```

2. Run deduplication from `runbook_reprocessing.sql`

#### Parse Errors

1. Check archive for raw JSON:
```sql
SELECT raw_json 
FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
WHERE originating_filename = 'problem-file'
LIMIT 10
```

2. Test JSON parsing:
```sql
SELECT SAFE.PARSE_JSON(raw_json) IS NOT NULL as is_valid
FROM `sbox-ravelar-001-20250926.logviewer.archive_ftplog`
WHERE gcs_uri = 'gs://bucket/logs/problem-file.json'
```

### Logs

**Scheduled Query**: BigQuery Console > Scheduled Queries > View Runs

**Cloud Function**:
```bash
gcloud functions logs read process-ftplog --gen2 --region=us-central1
```

---

## Configuration Reference

### config/settings.sh

| Variable | Description | Default |
|----------|-------------|---------|
| `PROJECT_ID` | GCP project ID | `sbox-ravelar-001-20250926` |
| `DATASET_ID` | BigQuery dataset | `logviewer` |
| `GCS_BUCKET` | GCS bucket name | `${PROJECT_ID}-ftplog` |
| `GCS_LOGS_PREFIX` | Folder for log files | `logs` |
| `SCHEDULED_QUERY_SCHEDULE` | ETL frequency | `every 5 minutes` |

### Source Data Format

Files must be NDJSON with this schema:

```json
{
  "UserName": "12345",
  "CustId": 12345,
  "PartnerName": null,
  "EventDt": "2026-01-28T10:30:00",
  "Action": "Store",
  "Filename": "/uploads/data.txt",
  "SessionId": "sess-abc123",
  "IpAddress": "192.168.1.100",
  "Source": "FTP-SERVER-01",
  "Bytes": 2048,
  "StatusCode": 226,
  "ServerResponse": "Closing data connection.",
  "RawData": "...",
  "HashCode": -1680792964
}
```

**Note**: `StatusCode` is intentionally NOT loaded to base table (per Snowflake parity requirement).

---

## Cleanup

To remove all pipeline resources:

```bash
./scripts/teardown.sh --confirm
```

⚠️ **Warning**: This deletes all data permanently!

---

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review monitoring queries in `sql/runbook_monitoring.sql`
3. Consult the original design document: `Design_and_planning.md`
