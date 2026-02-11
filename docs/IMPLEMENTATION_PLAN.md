# FTP Log Pipeline - Implementation Plan Summary

## Project Overview

This document summarizes the comprehensive GCP-native implementation for processing FTP log events, based on the requirements in `Design_and_planning.md`.

**Sandbox Project**: `sbox-ravelar-001-20250926`

---

## 📁 Deliverables

### Directory Structure

```
ftplog-pipeline/
├── config/
│   └── settings.sh                    # Centralized configuration
├── sql/
│   ├── 00_setup_all.sql              # One-click setup (all tables)
│   ├── 01_create_dataset.sql         # Dataset creation
│   ├── 02_create_base_table.sql      # Base table (partitioned/clustered)
│   ├── 03_create_archive_table.sql   # Archive table (raw JSON)
│   ├── 04_create_processed_table.sql # Tracking table (idempotency)
│   ├── 05_create_external_table.sql  # External table (GCS reader)
│   ├── 06_scheduled_query_etl.sql    # Main ETL logic
│   ├── validation_queries.sql        # QA validation queries
│   ├── runbook_reprocessing.sql      # Operational: reprocess scenarios
│   └── runbook_monitoring.sql        # Operational: monitoring queries
├── scripts/
│   ├── deploy.sh                     # Full deployment automation
│   ├── run_etl.sh                    # Manual ETL execution
│   ├── teardown.sh                   # Cleanup resources
│   └── deploy_cloud_function.sh      # Alternative: real-time deployment
├── cloud_function/
│   ├── main.py                       # Cloud Function (real-time alternative)
│   └── requirements.txt              # Python dependencies
├── tests/
│   ├── generate_test_data.py         # Test data generator
│   ├── test_pipeline.py              # Automated test suite
│   └── sample_data/                  # Sample NDJSON files
├── quickstart.sh                     # One-command setup + test
└── README.md                         # Complete documentation
```

---

## 🏗️ Architecture Decision

### Recommended: Scheduled Query (5-minute interval)

| Aspect | Implementation |
|--------|---------------|
| **Processing** | BigQuery Scheduled Query |
| **Latency** | 5-15 minutes |
| **Complexity** | Low (pure SQL) |
| **Cost** | Low (serverless) |
| **Maintenance** | Minimal |

### Alternative: Cloud Function (included but optional)

| Aspect | Implementation |
|--------|---------------|
| **Processing** | Cloud Function Gen2 |
| **Latency** | <1 minute |
| **Complexity** | Medium (Python code) |
| **When to Use** | Real-time requirements |

---

## 📊 BigQuery Schema Summary

### Tables Created

| Table | Purpose | Partitioning | Clustering |
|-------|---------|--------------|------------|
| `base_ftplog` | Structured events | `DATE(event_dt)` | `source, cust_id` |
| `archive_ftplog` | Raw JSON archive | `DATE(archived_timestamp)` | - |
| `processed_files` | Tracking (idempotency) | - | - |
| `external_ftplog_files` | GCS reader | - | - |

### Design Decisions

1. **Partitioning by `event_dt`**: Enables efficient date-range queries
2. **Clustering by `source, cust_id`**: Optimizes common filter patterns
3. **External Table for GCS**: No data duplication, always current
4. **StatusCode excluded**: Per Snowflake parity requirement (archived only)

---

## 🚀 Deployment Steps

### Quick Start (Recommended)

```bash
cd ftplog-pipeline
./quickstart.sh
```

### Manual Deployment

```bash
# 1. Configure settings
vim config/settings.sh

# 2. Deploy infrastructure
./scripts/deploy.sh

# 3. Generate and upload test data
python tests/generate_test_data.py -o ./test_files -n 5
gsutil cp ./test_files/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/

# 4. Run ETL
./scripts/run_etl.sh

# 5. Validate
bq query --use_legacy_sql=false < sql/validation_queries.sql
```

### Create Scheduled Query (in BigQuery Console)

1. Navigate to **BigQuery > Scheduled Queries**
2. Click **+ Create scheduled query**
3. Paste contents of `sql/06_scheduled_query_etl.sql`
4. Set schedule: **every 5 minutes**
5. Service account: `sa-logviewer@sbox-ravelar-001-20250926.iam.gserviceaccount.com`

---

## ✅ Validation Checklist

### Pre-Deployment
- [ ] GCP project exists: `sbox-ravelar-001-20250926`
- [ ] BigQuery API enabled
- [ ] Cloud Storage API enabled
- [ ] gcloud CLI authenticated

### Post-Deployment
- [ ] GCS bucket created: `gs://sbox-ravelar-001-20250926-ftplog`
- [ ] BigQuery dataset created: `logviewer`
- [ ] All 4 tables created
- [ ] Service account has correct IAM roles
- [ ] Test files processed successfully
- [ ] Data visible in `base_ftplog`
- [ ] Archive data in `archive_ftplog`
- [ ] Tracking records in `processed_files`

### Idempotency Test
- [ ] Re-run ETL produces no duplicates
- [ ] Same file not reprocessed

---

## 🔧 Operations

### Common Commands

```bash
# Check pipeline health
bq query --use_legacy_sql=false < sql/runbook_monitoring.sql

# Reprocess a file
# 1. Delete from processed_files
# 2. Delete from base_ftplog
# 3. Run ETL

# View recent processing
bq query --use_legacy_sql=false "
SELECT * FROM \`sbox-ravelar-001-20250926.logviewer.processed_files\`
ORDER BY processed_timestamp DESC LIMIT 10"
```

### Monitoring Alerts (Key Metrics)

| Metric | Threshold | Action |
|--------|-----------|--------|
| Files pending >15 min | >10 | Check scheduled query |
| Error rate | >5% | Review failed files |
| No processing in 1 hour | With pending files | Pipeline stuck |

---

## 🧹 Cleanup

```bash
./scripts/teardown.sh --confirm
```

⚠️ **Warning**: This permanently deletes all data!

---

## 📋 Requirements Mapping

| Requirement | Implementation | Status |
|-------------|---------------|--------|
| Parse NDJSON from GCS | External table + JSON_VALUE | ✅ |
| Insert structured data to base table | Scheduled query INSERT | ✅ |
| Archive raw JSON | archive_ftplog table | ✅ |
| Track processed files | processed_files table | ✅ |
| Prevent duplicate processing | EXISTS check in ETL | ✅ |
| Handle malformed JSON | SAFE_* functions | ✅ |
| 5-15 minute latency | 5-minute schedule | ✅ |
| Idempotent processing | processed_files ledger | ✅ |
| StatusCode excluded | Not in base schema | ✅ |
| Observability | Monitoring queries | ✅ |

---

## 📚 Files Reference

| File | Purpose | When to Use |
|------|---------|-------------|
| `quickstart.sh` | Full setup + test | First time setup |
| `deploy.sh` | Deploy infrastructure | Manual deployment |
| `run_etl.sh` | Execute ETL manually | Testing/debugging |
| `validation_queries.sql` | QA checks | After processing |
| `runbook_reprocessing.sql` | Reprocess files | Error recovery |
| `runbook_monitoring.sql` | Health checks | Ongoing monitoring |
| `test_pipeline.py` | Automated tests | CI/CD validation |

---

## Next Steps After Sandbox Testing

1. **Update project ID** in `config/settings.sh` for production
2. **Review IAM permissions** for production service account
3. **Set up Cloud Monitoring alerts** using monitoring queries
4. **Configure .NET service** with `UseBigQueryMode = true`
5. **Create scheduled query** in production BigQuery
6. **Document runbook** for operations team
