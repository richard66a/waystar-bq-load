# FTP Log Pipeline

A production-ready, GCP-native data pipeline for processing FTP log events from GCS to BigQuery.

## Overview

This pipeline processes FTP log events stored as NDJSON files in Google Cloud Storage and loads them into BigQuery for analysis. It features:

- **GCP-Native**: Pure SQL-based ETL using BigQuery scheduled queries
- **Idempotent**: Files are tracked to prevent duplicate processing
- **Archive-First**: Raw JSON preserved for compliance and recovery
- **Scalable**: Handles 100-1000 files/day with minimal configuration

## Quick Start

```bash
# 1. Configure
cp config/example.settings.sh config/settings.sh
vim config/settings.sh  # Set PROJECT_ID, GCS_BUCKET, DATASET_ID

# 2. Deploy
source config/settings.sh
./scripts/deploy.sh

# 3. Test
./scripts/e2e_gcp_test.sh
```

## Documentation

See [docs/README.md](docs/README.md) for full documentation:

| Document | Description |
|----------|-------------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design and data flow |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Setup and deployment steps |
| [docs/OPERATIONS.md](docs/OPERATIONS.md) | Monitoring and troubleshooting |
| [docs/TESTING.md](docs/TESTING.md) | Running tests and validation |
| [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) | Development workflow |

## Project Structure

```
├── config/           # Environment configuration
│   ├── example.settings.sh  # Template (copy to settings.sh)
│   └── settings.sh          # Your configuration (git-ignored)
├── sql/              # BigQuery SQL scripts
│   ├── 00_setup_all.sql     # Combined setup script
│   ├── 06_scheduled_query_proc.sql  # ETL stored procedure
│   ├── validation_queries.sql       # Data validation
│   └── runbook_*.sql                # Operational runbooks
├── scripts/          # Bash deployment and utility scripts
│   ├── deploy.sh            # Full deployment
│   ├── run_etl.sh           # Manual ETL execution
│   ├── e2e_gcp_test.sh      # End-to-end test
│   └── teardown.sh          # Cleanup resources
├── cloud_function/   # Python Cloud Function (optional)
├── tests/            # Test suite and sample data
└── docs/             # Documentation
```

## Configuration

All configuration is managed through environment variables. Copy the template and set your values:

```bash
cp config/example.settings.sh config/settings.sh
```

Required variables:

| Variable | Description |
|----------|-------------|
| `PROJECT_ID` | GCP project ID |
| `GCS_BUCKET` | GCS bucket for log files |
| `DATASET_ID` | BigQuery dataset name |

## Testing

```bash
# Smoke tests (no GCP required)
python3 -m pytest -q

# Local sample validation
python3 tests/validate_local_samples.py tests/sample_data/*.json

# End-to-end GCP test
export PROJECT_ID=your-project-id
./scripts/e2e_gcp_test.sh
```

## License

Internal use only.
