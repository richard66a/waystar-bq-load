# Deployment

## Prerequisites

- Google Cloud SDK (`gcloud`, `bq`, `gsutil`) installed and authenticated
- GCP project with BigQuery and Cloud Storage APIs enabled
- Appropriate IAM permissions (see below)

## Configuration

1. Copy the example config and set your values:

```bash
cp config/example.settings.sh config/settings.sh
vim config/settings.sh
```

Required variables:
- `PROJECT_ID`: Your GCP project ID
- `GCS_BUCKET`: GCS bucket name (defaults to `${PROJECT_ID}-ftplog`)
- `DATASET_ID`: BigQuery dataset name (defaults to `logviewer`)

2. Source the config before running any scripts:

```bash
source config/settings.sh
```

## Deployment Steps

### Option A: Full Deployment (Recommended)

```bash
chmod +x scripts/*.sh
./scripts/deploy.sh
```

This script:
- Creates the GCS bucket
- Creates BigQuery dataset and tables
- Sets up the external table
- Creates the ETL stored procedure
- Configures service account and IAM

### Option B: Manual Deployment

1. Create GCS bucket:
```bash
gsutil mb -p ${PROJECT_ID} -l US gs://${GCS_BUCKET}
```

2. Create BigQuery resources:
```bash
# Replace tokens and run setup
sed "s|__PROJECT_ID__|${PROJECT_ID}|g; s|__DATASET_ID__|${DATASET_ID}|g; s|__GCS_URI__|gs://${GCS_BUCKET}/logs/*.json|g" \
  sql/00_setup_all.sql | bq query --use_legacy_sql=false
```

3. Create ETL stored procedure:
```bash
sed "s|__PROJECT_ID__|${PROJECT_ID}|g; s|__DATASET_ID__|${DATASET_ID}|g" \
  sql/06_scheduled_query_proc.sql | bq query --use_legacy_sql=false
```

### Creating the Scheduled Query

To run ETL automatically every 5 minutes:

```bash
./scripts/create_scheduled_query.sh --call-proc --apply
```

Or use Cloud Scheduler with an HTTP function:
```bash
./scripts/deploy_scheduled_etl.sh
```

### Deploying Cloud Function (Real-time Alternative)

For sub-minute latency:

```bash
./scripts/deploy_cloud_function.sh
```

## IAM Requirements

The service account needs these roles:

| Role | Purpose |
|------|---------|
| `roles/bigquery.dataEditor` | Write to BigQuery tables |
| `roles/bigquery.jobUser` | Run BigQuery jobs |
| `roles/storage.objectViewer` | Read from GCS bucket |

For scheduled queries, also add:
- `roles/bigquery.admin` (to create scheduled queries)

## Environment Configuration

For different environments (dev/staging/prod), create separate config files:

```bash
# Development
cp config/example.settings.sh config/settings.dev.sh
# Edit with dev project values

# Staging
cp config/example.settings.sh config/settings.staging.sh
# Edit with staging project values

# Production
cp config/example.settings.sh config/settings.prod.sh
# Edit with production project values
```

Then source the appropriate file:
```bash
source config/settings.prod.sh
./scripts/deploy.sh
```

## Validation

After deployment, verify:

```bash
# Check tables exist
bq ls ${PROJECT_ID}:${DATASET_ID}

# Check external table can read GCS
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`${PROJECT_ID}.${DATASET_ID}.external_ftplog_files\`"

# Run the E2E test
./scripts/e2e_gcp_test.sh
```
