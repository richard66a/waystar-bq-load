#!/bin/bash
# =============================================================================
# FTP Log Pipeline - Quick Start Script
# =============================================================================
# Run this script to quickly set up and test the entire pipeline.
# 
# Usage:
#   chmod +x quickstart.sh
#   ./quickstart.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo "=============================================================="
echo "FTP Log Pipeline - Quick Start"
echo "=============================================================="
echo ""
echo "This script will:"
echo "  1. Deploy all GCP infrastructure"
echo "  2. Generate test data"
echo "  3. Upload test data to GCS"
echo "  4. Run the ETL pipeline"
echo "  5. Validate the results"
echo ""
read -p "Continue? (y/n): " CONFIRM
if [ "${CONFIRM}" != "y" ]; then
    echo "Aborted."
    exit 0
fi

# Step 1: Deploy infrastructure
echo ""
echo "Step 1: Deploying infrastructure..."
echo "------------------------------------------------------------"
./scripts/deploy.sh

# Step 2: Generate test data
echo ""
echo "Step 2: Generating test data..."
echo "------------------------------------------------------------"
python3 tests/generate_test_data.py \
    --output-dir ./test_files \
    --num-files 3 \
    --rows-per-file 50 \
    --seed 42

# Step 3: Upload test data
echo ""
echo "Step 3: Uploading test data to GCS..."
echo "------------------------------------------------------------"
source config/settings.sh
gsutil cp ./test_files/*.json "gs://${GCS_BUCKET}/${GCS_LOGS_PREFIX}/"

# Step 4: Run ETL
echo ""
echo "Step 4: Running ETL pipeline..."
echo "------------------------------------------------------------"
./scripts/run_etl.sh

# Step 5: Validate
echo ""
echo "Step 5: Validating results..."
echo "------------------------------------------------------------"
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" \
    "SELECT 'Files Processed' as metric, COUNT(*) as value 
     FROM \`${FQ_PROCESSED_TABLE}\`
     UNION ALL
     SELECT 'Rows in Base Table', COUNT(*) 
     FROM \`${FQ_BASE_TABLE}\`
     UNION ALL  
     SELECT 'Rows in Archive', COUNT(*) 
     FROM \`${FQ_ARCHIVE_TABLE}\`"

echo ""
echo "=============================================================="
echo "Quick Start Complete!"
echo "=============================================================="
echo ""
echo "Next steps:"
echo "  - View data in BigQuery Console"
echo "  - Run validation queries: bq query < sql/validation_queries.sql"
echo "  - Run test suite: python3 tests/test_pipeline.py"
echo "  - Set up scheduled query in BigQuery Console"
echo ""
echo "To clean up: ./scripts/teardown.sh --confirm"
echo ""
