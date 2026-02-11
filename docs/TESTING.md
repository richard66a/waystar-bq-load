# Testing

## Quick Start

```bash
# Run smoke tests (no GCP required)
python3 -m pytest -q

# Validate sample data files
python3 tests/validate_local_samples.py tests/sample_data/*.json

# Run E2E test (requires GCP credentials)
export PROJECT_ID=your-project-id
./scripts/e2e_gcp_test.sh
```

## Test Types

### Unit/Smoke Tests

Fast, offline tests that verify code structure:

```bash
python3 -m pytest -q
```

These tests:
- Check that modules import correctly
- Verify test utilities exist
- Do not connect to GCP

### Local Sample Validation

Validates NDJSON files match expected schema:

```bash
python3 tests/validate_local_samples.py tests/sample_data/*.json
```

Checks:
- JSON is parseable
- Required fields present
- `EventDt` is valid ISO 8601 with trailing Z
- `HashCode` is a valid 64-bit integer

### End-to-End GCP Test

Full pipeline validation against a real GCP project:

```bash
export PROJECT_ID=your-project-id
export DATASET=logviewer
export BUCKET=${PROJECT_ID}-ftplog

./scripts/e2e_gcp_test.sh
```

This script:
1. Uploads sample file to GCS
2. Creates/updates BigQuery resources
3. Runs the ETL stored procedure
4. Validates `processed_files` ledger
5. Checks row counts in `base_ftplog` and `archive_ftplog`

### Full Pipeline Test Suite

For comprehensive testing with custom settings:

```bash
python3 tests/test_pipeline.py --project $PROJECT_ID --bucket $GCS_BUCKET --dataset $DATASET_ID
```

Tests include:
- GCS connectivity
- BigQuery connectivity
- External table functionality
- File processing flow
- Idempotency verification

## Generating Test Data

Create sample NDJSON files:

```bash
# Basic test files
python3 tests/generate_test_data.py -o ./test_files -n 5 -r 100

# With malformed JSON for error testing
python3 tests/generate_test_data.py -o ./test_files -n 3 -r 50 --include-malformed

# Reproducible output with seed
python3 tests/generate_test_data.py -o ./test_files -n 3 --seed 42
```

Upload to GCS:
```bash
gsutil cp ./test_files/*.json gs://${GCS_BUCKET}/logs/
```

## Validation Queries

Run the validation query suite:

```bash
# Replace tokens and execute
sed "s|__PROJECT_ID__|${PROJECT_ID}|g; s|__DATASET_ID__|${DATASET_ID}|g" \
  sql/validation_queries.sql | bq query --use_legacy_sql=false
```

Validation checks:
- Pipeline status (success/failure counts)
- Pending files
- Parse errors
- Duplicate detection
- Data type validation
- Archive completeness

## Test Files

- `tests/sample_data/`: Pre-generated sample NDJSON files
- `tests/generate_test_data.py`: Test data generator
- `tests/validate_local_samples.py`: Local file validator
- `tests/test_pipeline.py`: Full pipeline test suite
- `tests/test_smoke.py`: Pytest smoke tests
