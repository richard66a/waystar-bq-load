# FTP Log Pipeline Documentation

This folder contains the documentation for the GCP-native FTP log pipeline.

## Quick Links

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, data flow, and table schemas |
| [DEPLOYMENT.md](DEPLOYMENT.md) | Setup, configuration, and deployment steps |
| [OPERATIONS.md](OPERATIONS.md) | Monitoring, reprocessing, and troubleshooting |
| [TESTING.md](TESTING.md) | Running tests, generating test data, validation |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development workflow, code standards, PR checklist |

## SQL References

| File | Purpose |
|------|---------|
| [../sql/00_setup_all.sql](../sql/00_setup_all.sql) | Combined setup script |
| [../sql/06_scheduled_query_proc.sql](../sql/06_scheduled_query_proc.sql) | ETL stored procedure |
| [../sql/validation_queries.sql](../sql/validation_queries.sql) | Data validation queries |
| [../sql/runbook_monitoring.sql](../sql/runbook_monitoring.sql) | Monitoring queries |
| [../sql/runbook_reprocessing.sql](../sql/runbook_reprocessing.sql) | Reprocessing scenarios |

## Getting Started

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) to understand the system
2. Follow [DEPLOYMENT.md](DEPLOYMENT.md) to set up your environment
3. Use [TESTING.md](TESTING.md) to validate the pipeline
4. Refer to [OPERATIONS.md](OPERATIONS.md) for ongoing maintenance

