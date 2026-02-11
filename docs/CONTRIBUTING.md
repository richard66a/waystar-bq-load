# Contributing

## Getting Started

1. Clone the repository
2. Copy config template: `cp config/example.settings.sh config/settings.sh`
3. Edit `config/settings.sh` with your GCP project details
4. Run smoke tests: `python3 -m pytest -q`

## Code Structure

```
├── config/           # Environment configuration
├── sql/              # BigQuery SQL scripts
├── scripts/          # Bash deployment and utility scripts
├── cloud_function/   # Python Cloud Function (optional)
├── tests/            # Test suite and sample data
└── docs/             # Documentation
```

## Development Workflow

1. Create a feature branch
2. Make changes
3. Run local tests: `python3 -m pytest -q`
4. Validate sample data: `python3 tests/validate_local_samples.py tests/sample_data/*.json`
5. If SQL changes, test against a sandbox project
6. Submit a pull request

## Code Standards

### SQL

- Use `CREATE OR REPLACE` for procedures
- Use `CREATE TABLE IF NOT EXISTS` for tables
- Include field descriptions using `OPTIONS(description = '...')`
- Use `SAFE_*` functions for fault-tolerant parsing
- Parameterize project/dataset with `__PROJECT_ID__` and `__DATASET_ID__` tokens

### Python

- Follow PEP 8 style guidelines
- Add Google-style docstrings for public functions
- Use type hints for function signatures
- Default configuration should come from environment variables

### Shell Scripts

- Start with `#!/usr/bin/env bash`
- Use `set -euo pipefail` for strict mode
- Source `config/settings.sh` for configuration
- Use the helper functions: `log_info`, `log_error`, `log_success`, `require_cmd`

## Pull Request Checklist

Before submitting a PR:

- [ ] No hardcoded project IDs, bucket names, or secrets
- [ ] SQL uses `__PROJECT_ID__`/`__DATASET_ID__` tokens
- [ ] Python uses environment variables for configuration
- [ ] Local tests pass: `python3 -m pytest -q`
- [ ] Sample validation passes: `python3 tests/validate_local_samples.py tests/sample_data/*.json`
- [ ] Documentation updated if needed
- [ ] No merge conflicts with main branch

## Testing Changes

### Local Testing
```bash
python3 -m pytest -q
python3 tests/validate_local_samples.py tests/sample_data/*.json
```

### GCP Testing
```bash
export PROJECT_ID=your-sandbox-project
./scripts/e2e_gcp_test.sh
```

## Deployment Checklist

Before deploying to production:

- [ ] All tests pass in staging environment
- [ ] IAM permissions reviewed
- [ ] Monitoring alerts configured
- [ ] Rollback plan documented
- [ ] Team notified of deployment window
