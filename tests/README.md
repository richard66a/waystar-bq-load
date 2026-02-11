# Tests Module

Test suite and sample data for validating the FTP Log Pipeline.

## Overview

This module contains:
- End-to-end pipeline tests
- Test data generators
- Sample validation utilities
- Sample NDJSON files

## Running Tests

### Local Smoke Tests (No GCP Required)

```bash
# Run all local tests
python3 -m pytest -q

# Validate sample files
python3 validate_local_samples.py sample_data/*.json
```

### End-to-End GCP Tests

Requires GCP authentication and configured environment:

```bash
export PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket
export DATASET_ID=logviewer

python3 test_pipeline.py --project $PROJECT_ID --bucket $GCS_BUCKET
```

Or use the wrapper script:

```bash
../scripts/e2e_gcp_test.sh
```

## Test Files

| File | Description |
|------|-------------|
| `test_pipeline.py` | End-to-end GCP integration tests |
| `generate_test_data.py` | Generate realistic NDJSON test files |
| `validate_local_samples.py` | Validate NDJSON files locally |
| `validate_sample.sh` | Shell wrapper for validation |
| `sample_data/` | Pre-generated sample files |

## Test Classes

The test suite includes:

| Test | Description |
|------|-------------|
| `TestGCSConnectivity` | Verify GCS bucket access |
| `TestBigQueryConnectivity` | Verify BigQuery access |
| `TestExternalTable` | Verify external table reads from GCS |
| `TestFileProcessing` | End-to-end file upload → ETL → verify |
| `TestIdempotency` | Verify no duplicates on re-run |

## Generating Test Data

```bash
# Generate 3 files with 100 rows each
python3 generate_test_data.py \
    --output-dir ./test_files \
    --num-files 3 \
    --rows-per-file 100

# Upload to GCS
gsutil cp ./test_files/*.json gs://${GCS_BUCKET}/logs/
```

## Sample Data Format

Test files follow the NDJSON format with one JSON object per line:

```json
{"UserName":"12345","CustId":12345,"EventDt":"2025-02-04T12:00:00Z","Action":"Store","Filename":"/uploads/file.txt","SessionId":"sess-abc123","IpAddress":"192.168.1.1","Source":"FTP-SERVER-01","Bytes":1024,"StatusCode":226}
```

Required fields:
- `UserName`, `CustId`, `EventDt`, `Action`, `Filename`
- `SessionId`, `IpAddress`, `Source`, `Bytes`, `StatusCode`

## Validation

The `validate_local_samples.py` script checks:
- JSON parseability
- Required field presence
- Type expectations (integers for `CustId`, `Bytes`, `StatusCode`)
- Timestamp format (`EventDt` as ISO 8601)
