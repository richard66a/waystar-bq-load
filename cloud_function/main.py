"""
FTP Log Pipeline - Cloud Function (Real-time Processing)
========================================================
Alternative to scheduled query for near real-time (<1 min) processing.

This Cloud Function is triggered when a new JSON file is uploaded to GCS.
It parses the NDJSON file and loads data into BigQuery.

When to use this instead of scheduled query:
- Need <1 minute latency
- Need per-file error handling and retries
- Complex per-file logic not expressible in SQL

Deployment:
    cd cloud_function
    gcloud functions deploy process-ftplog \
        --gen2 \
        --runtime=python311 \
        --region=us-central1 \
        --source=. \
        --entry-point=process_ftplog \
        --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
        --trigger-event-filters="bucket=sbox-ravelar-001-20250926-ftplog" \
        --service-account=sa-logviewer@sbox-ravelar-001-20250926.iam.gserviceaccount.com \
        --memory=512MB \
        --timeout=300s
"""

import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import json
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

PROJECT_ID = "sbox-ravelar-001-20250926"
DATASET_ID = "logviewer"
BASE_TABLE = "base_ftplog"
ARCHIVE_TABLE = "archive_ftplog"
PROCESSED_TABLE = "processed_files"

# Fully qualified table names
FQ_BASE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{BASE_TABLE}"
FQ_ARCHIVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{ARCHIVE_TABLE}"
FQ_PROCESSED_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{PROCESSED_TABLE}"

# SQL file for scheduled ETL (HTTP-triggered function)
SCHEDULED_SQL_PATH = Path(__file__).parent / "etl_sql.sql"

# File pattern to process (only files in logs/ prefix with .json extension)
FILE_PATTERN = r"^logs/.*\.json$"


# =============================================================================
# Helper Functions
# =============================================================================

def extract_originating_filename(gcs_uri: str) -> str:
    """Extract the filename (without extension) from GCS URI."""
    match = re.search(r'/([^/]+)\.json$', gcs_uri)
    return match.group(1) if match else "unknown"


def parse_event_timestamp(event_dt_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 timestamp from JSON."""
    if not event_dt_str:
        return None
    
    # Handle various ISO 8601 formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(event_dt_str, fmt)
        except ValueError:
            continue
    
    logger.warning(f"Could not parse timestamp: {event_dt_str}")
    return None


def safe_int(value: Any) -> Optional[int]:
    """Safely convert value to int."""
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_json_line(line: str, gcs_uri: str, originating_filename: str) -> Tuple[Optional[Dict], Dict]:
    """
    Parse a single JSON line into base table row and archive row.
    
    Returns:
        Tuple of (base_row, archive_row) where base_row may be None if parsing fails
    """
    load_time = datetime.utcnow()
    
    # Archive row always includes the raw JSON
    archive_row = {
        "raw_json": line,
        "archived_timestamp": load_time.isoformat(),
        "process_dt": load_time.isoformat(),
        "originating_filename": originating_filename,
        "gcs_uri": gcs_uri,
    }
    
    # Try to parse JSON for base table
    try:
        data = json.loads(line)
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e} - Line: {line[:100]}...")
        return None, archive_row
    
    # Parse event timestamp
    event_dt = parse_event_timestamp(data.get("EventDt"))
    
    base_row = {
        "load_time_dt": load_time.isoformat(),
        "source_file_dt": event_dt.isoformat() if event_dt else load_time.isoformat(),
        "originating_filename": originating_filename,
        "gcs_uri": gcs_uri,
        "action": data.get("Action"),
        "bytes": safe_int(data.get("Bytes")),
        "cust_id": safe_int(data.get("CustId")),
        "event_dt": event_dt.isoformat() if event_dt else None,
        "filename": data.get("Filename"),
        "hash_code": safe_int(data.get("HashCode")),
        "ip_address": data.get("IpAddress"),
        "partner_name": data.get("PartnerName"),
        "session_id": data.get("SessionId"),
        "source": data.get("Source"),
        "user_name": data.get("UserName"),
        "server_response": data.get("ServerResponse"),
        "raw_data": data.get("RawData"),
        # NOTE: StatusCode intentionally excluded per requirements
    }
    
    return base_row, archive_row


def is_already_processed(client: bigquery.Client, gcs_uri: str) -> bool:
    """Check if file has already been processed."""
    query = f"""
        SELECT 1 FROM `{FQ_PROCESSED_TABLE}`
        WHERE gcs_uri = @gcs_uri
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("gcs_uri", "STRING", gcs_uri)
        ]
    )
    results = list(client.query(query, job_config=job_config).result())
    return len(results) > 0


def load_to_bigquery(
    client: bigquery.Client,
    base_rows: List[Dict],
    archive_rows: List[Dict],
    gcs_uri: str,
    originating_filename: str
) -> str:
    """Load parsed data into BigQuery tables."""
    start_time = datetime.utcnow()
    
    # Insert into base table
    if base_rows:
        errors = client.insert_rows_json(FQ_BASE_TABLE, base_rows)
        if errors:
            logger.error(f"Base table insert errors: {errors}")
            raise Exception(f"Failed to insert into base table: {errors}")
    
    # Insert into archive table
    if archive_rows:
        errors = client.insert_rows_json(FQ_ARCHIVE_TABLE, archive_rows)
        if errors:
            logger.error(f"Archive table insert errors: {errors}")
            raise Exception(f"Failed to insert into archive table: {errors}")
    
    # Calculate processing duration
    end_time = datetime.utcnow()
    duration_seconds = (end_time - start_time).total_seconds()
    
    # Record in processed_files
    processed_row = [{
        "gcs_uri": gcs_uri,
        "originating_filename": originating_filename,
        "processed_timestamp": end_time.isoformat(),
        "rows_loaded": len(base_rows),
        "status": "SUCCESS" if len(base_rows) == len(archive_rows) else "PARTIAL",
        "processing_duration_seconds": duration_seconds,
    }]
    
    errors = client.insert_rows_json(FQ_PROCESSED_TABLE, processed_row)
    if errors:
        logger.error(f"Processed files insert errors: {errors}")
        raise Exception(f"Failed to record processed file: {errors}")
    
    return "SUCCESS" if len(base_rows) == len(archive_rows) else "PARTIAL"


# =============================================================================
# Cloud Function Entry Point
# =============================================================================

@functions_framework.cloud_event
def process_ftplog(cloud_event):
    """
    Cloud Function triggered by GCS object finalize event.
    
    Processes new NDJSON files and loads data into BigQuery.
    """
    data = cloud_event.data
    bucket = data["bucket"]
    name = data["name"]
    gcs_uri = f"gs://{bucket}/{name}"
    
    logger.info(f"Processing file: {gcs_uri}")
    
    # Check if this is a file we should process
    if not re.match(FILE_PATTERN, name):
        logger.info(f"Skipping non-target file: {name}")
        return
    
    # Skip placeholder files
    if name.endswith(".placeholder"):
        logger.info(f"Skipping placeholder file: {name}")
        return
    
    # Initialize clients
    bq_client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)
    
    # Check if already processed (idempotency)
    if is_already_processed(bq_client, gcs_uri):
        logger.info(f"File already processed: {gcs_uri}")
        return
    
    # Extract metadata
    originating_filename = extract_originating_filename(gcs_uri)
    logger.info(f"Originating filename: {originating_filename}")
    
    # Read file from GCS
    bucket_obj = storage_client.bucket(bucket)
    blob = bucket_obj.blob(name)
    content = blob.download_as_text(encoding="utf-8")
    
    # Parse lines
    base_rows = []
    archive_rows = []
    parse_errors = 0
    
    for line in content.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        
        base_row, archive_row = parse_json_line(line, gcs_uri, originating_filename)
        
        archive_rows.append(archive_row)
        if base_row:
            base_rows.append(base_row)
        else:
            parse_errors += 1
    
    logger.info(f"Parsed {len(base_rows)} valid rows, {parse_errors} parse errors")
    
    # Load to BigQuery
    try:
        status = load_to_bigquery(
            bq_client,
            base_rows,
            archive_rows,
            gcs_uri,
            originating_filename
        )
        logger.info(f"Successfully processed {gcs_uri} - Status: {status}, Rows: {len(base_rows)}")
    except Exception as e:
        logger.error(f"Failed to load {gcs_uri}: {e}")
        # Record failure
        try:
            failed_row = [{
                "gcs_uri": gcs_uri,
                "originating_filename": originating_filename,
                "processed_timestamp": datetime.utcnow().isoformat(),
                "rows_loaded": 0,
                "status": "FAILED",
                "error_message": str(e)[:1000],
            }]
            bq_client.insert_rows_json(FQ_PROCESSED_TABLE, failed_row)
        except Exception as record_error:
            logger.error(f"Failed to record failure: {record_error}")
        raise


# =============================================================================
# Scheduled ETL (HTTP-triggered)
# =============================================================================

@functions_framework.http
def run_scheduled_etl(request):
    """
    HTTP-triggered function to run the multi-statement ETL script.
    Intended to be called by Cloud Scheduler every 5 minutes.
    """
    if not SCHEDULED_SQL_PATH.exists():
        logger.error(f"Missing SQL file: {SCHEDULED_SQL_PATH}")
        return ("Missing SQL file", 500)

    sql = SCHEDULED_SQL_PATH.read_text()
    bq_client = bigquery.Client(project=PROJECT_ID)

    try:
        job = bq_client.query(sql)
        job.result()
        logger.info(f"Scheduled ETL completed. Job ID: {job.job_id}")
        return ({"status": "SUCCESS", "job_id": job.job_id}, 200)
    except Exception as exc:
        logger.error(f"Scheduled ETL failed: {exc}")
        return ({"status": "FAILED", "error": str(exc)}, 500)


# =============================================================================
# Local Testing Entry Point
# =============================================================================

def test_locally(bucket: str, file_name: str):
    """
    Test the function locally without Cloud Functions framework.
    
    Usage:
        python main.py --bucket sbox-ravelar-001-20250926-ftplog --file logs/test-file.json
    """
    from unittest.mock import MagicMock
    
    # Create mock cloud event
    mock_event = MagicMock()
    mock_event.data = {
        "bucket": bucket,
        "name": file_name,
    }
    
    logger.info(f"Testing locally with bucket={bucket}, file={file_name}")
    process_ftplog(mock_event)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Cloud Function locally")
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--file", required=True, help="File path within bucket")
    
    args = parser.parse_args()
    test_locally(args.bucket, args.file)
