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
        --trigger-event-filters="bucket=<YOUR_BUCKET>" \
        --service-account=<YOUR_SERVICE_ACCOUNT> \
        --memory=512MB \
        --timeout=300s

Environment variables:
    PROJECT_ID, DATASET_ID, GCS_LOGS_PREFIX
"""

import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import json
import re
import logging
import os
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("DATASET_ID", "logviewer")
BASE_TABLE = os.getenv("BASE_TABLE", "base_ftplog")
ARCHIVE_TABLE = os.getenv("ARCHIVE_TABLE", "archive_ftplog")
PROCESSED_TABLE = os.getenv("PROCESSED_TABLE", "processed_files")
GCS_LOGS_PREFIX = os.getenv("GCS_LOGS_PREFIX", "logs")

# Fully qualified table names
FQ_BASE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{BASE_TABLE}"
FQ_ARCHIVE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{ARCHIVE_TABLE}"
FQ_PROCESSED_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{PROCESSED_TABLE}"

# SQL file for scheduled ETL (HTTP-triggered function)
SCHEDULED_SQL_PATH = Path(__file__).parent / "etl_sql.sql"

# File pattern to process (only files in logs/ prefix with .json extension)
FILE_PATTERN = rf"^{re.escape(GCS_LOGS_PREFIX)}/.*\.json$"


# =============================================================================
# Helper Functions
# =============================================================================

def extract_originating_filename(gcs_uri: str) -> str:
    """Extract the originating filename (without extension) from a GCS URI.

    Args:
        gcs_uri: Full GCS URI or path component (for example,
            "gs://bucket/logs/myfile.json" or "logs/myfile.json").

    Returns:
        The filename without the ``.json`` extension if present, otherwise
        the string ``"unknown"``.

    Examples:
        >>> extract_originating_filename('gs://bucket/logs/foo.json')
        'foo'
    """
    match = re.search(r'/([^/]+)\.json$', gcs_uri)
    return match.group(1) if match else "unknown"


def parse_event_timestamp(event_dt_str: Optional[str]) -> Optional[datetime]:
    """Parse an ISO 8601 timestamp string into a ``datetime`` object.

    The function handles several common ISO 8601 variants with and without
    fractional seconds and trailing ``Z`` UTC designator. If the input is
    falsy or cannot be parsed, ``None`` is returned.

    Args:
        event_dt_str: Timestamp string (example: ``"2026-01-28T10:30:00.000Z"``)

    Returns:
        A ``datetime`` instance when parsing succeeds, otherwise ``None``.
    """
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
    """Safely convert a value to an ``int`` when possible.

    This helper returns ``None`` if the input is ``None`` or cannot be
    converted to an integer (for example, non-numeric strings).

    Args:
        value: Value to convert (may be ``str``, ``int``, or other).

    Returns:
        The converted integer, or ``None`` if conversion failed.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def compute_hash_fingerprint(data: Dict[str, Any]) -> str:
    """Compute a deterministic SHA256 hex fingerprint from canonical fields.

    The fingerprint is computed by concatenating selected JSON fields in a
    deterministic order with a ``|`` separator and hashing the result using
    SHA256. This fingerprint is used for row-level deduplication in the
    ETL pipeline.

    Args:
        data: Parsed JSON object (dictionary) containing keys such as
            ``EventDt``, ``Source``, ``Filename``, ``Bytes``, and ``UserName``.

    Returns:
        Hexadecimal SHA256 fingerprint string.
    """
    canonical = "|".join([
        str(data.get("EventDt") or ""),
        str(data.get("Source") or ""),
        str(data.get("Filename") or ""),
        str(data.get("Bytes") or ""),
        str(data.get("UserName") or ""),
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_json_line(line: str, gcs_uri: str, originating_filename: str) -> Tuple[Optional[Dict], Dict]:
    """Parse a single NDJSON line and produce rows for base and archive tables.

    The function always returns an ``archive_row`` containing the original
    raw JSON and metadata. If the JSON line can be parsed and contains
    expected fields, a structured ``base_row`` dictionary is also returned.
    On JSON parse failure, ``base_row`` is ``None`` and only the
    ``archive_row`` is provided.

    Args:
        line: A single line of NDJSON (string).
        gcs_uri: The full GCS URI for the source file.
        originating_filename: The extracted filename (without extension) used
            as a load identifier.

    Returns:
        A tuple ``(base_row, archive_row)`` where ``base_row`` is a mapping
        suitable for insertion into ``base_ftplog`` (or ``None`` on parse
        failure), and ``archive_row`` is a mapping suitable for
        insertion into ``archive_ftplog``.
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
        "hash_fingerprint": compute_hash_fingerprint(data),
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
    """Return whether a given GCS URI has already been recorded as processed.

    This helper queries the ``processed_files`` ledger (``FQ_PROCESSED_TABLE``)
    to determine idempotency for a particular file. It performs a parameterized
    BigQuery query to avoid injection and to be efficient at scale.

    Args:
        client: An initialized ``google.cloud.bigquery.Client`` for queries.
        gcs_uri: Full GCS URI of the file to check.

    Returns:
        ``True`` if an entry exists for the provided ``gcs_uri``, otherwise
        ``False``.
    """
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
    """Insert parsed rows into BigQuery and record processing metadata.

    The function performs three actions in sequence:
    1. Insert structured rows into the ``base`` table (if any).
    2. Insert raw rows into the ``archive`` table (if any).
    3. Write a summary row into the ``processed_files`` ledger recording
       counts, status and processing duration.

    On any insertion error into the primary tables, an exception is raised to
    allow the caller to handle retries and error recording.

    Args:
        client: Initialized ``google.cloud.bigquery.Client`` used to insert
            and write the processed_files ledger.
        base_rows: List of structured rows to insert into ``base_ftplog``.
        archive_rows: List of raw JSON rows to insert into ``archive_ftplog``.
        gcs_uri: Full GCS URI of the processed file.
        originating_filename: Filename identifier used in the processed_files
            ledger.

    Returns:
        A string status value ("SUCCESS", "PARTIAL", or "FAILED")
        describing the outcome.

    Raises:
        Exception: If inserts into the base or archive tables fail, or if the
            processed_files ledger insert fails.
    """
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
    rows_expected = len(archive_rows)
    rows_loaded = len(base_rows)
    parse_errors = max(rows_expected - rows_loaded, 0)
    status = "SUCCESS" if parse_errors == 0 and rows_loaded > 0 else "PARTIAL"
    error_message = None
    if rows_expected == 0:
        status = "FAILED"
        error_message = "No non-empty rows found in file"
    elif rows_loaded == 0:
        status = "FAILED"
        error_message = "No rows loaded from file"
    elif parse_errors > 0:
        error_message = f"Parsed {rows_loaded} of {rows_expected} rows"

    processed_row = [{
        "gcs_uri": gcs_uri,
        "originating_filename": originating_filename,
        "processed_timestamp": end_time.isoformat(),
        "rows_loaded": rows_loaded,
        "rows_expected": rows_expected,
        "parse_errors": parse_errors,
        "status": status,
        "error_message": error_message,
        "processing_duration_seconds": duration_seconds,
    }]

    errors = client.insert_rows_json(FQ_PROCESSED_TABLE, processed_row)
    if errors:
        logger.error(f"Processed files insert errors: {errors}")
        raise Exception(f"Failed to record processed file: {errors}")

    return status


# =============================================================================
# Cloud Function Entry Point
# =============================================================================

@functions_framework.cloud_event
def process_ftplog(cloud_event):
    """
    Cloud Function entry point for GCS "object finalize" events.

    This function is executed when a new object is finalized in the configured
    GCS bucket. It performs the following steps:
    1. Validates the object path matches the configured file pattern.
    2. Skips placeholders and already-processed files (idempotency check).
    3. Downloads the NDJSON content, parses each line and builds rows for
       both the structured ``base`` table and the raw ``archive`` table.
    4. Calls ``load_to_bigquery`` to persist rows and record processing
       metadata in ``processed_files``.

    Any insert errors into BigQuery cause the function to attempt to record
    a failure row in the ``processed_files`` table before re-raising the
    exception so that Cloud Functions/Cloud Logging can surface the failure.

    Args:
        cloud_event: The CloudEvents v1 payload delivered by Cloud Functions
            for storage events (dictionary-like with keys ``bucket`` and
            ``name``).

    Returns:
        None. Successful processing is logged; failures raise exceptions.
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
# Monitoring Alert (HTTP-triggered)
# =============================================================================

@functions_framework.http
def run_monitoring_alert(request):
    """
    HTTP-triggered function to read the latest pipeline_monitoring snapshot
    and emit a log entry if status='ALERT'. Intended for Cloud Scheduler.
    """
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT status, details
        FROM `{PROJECT_ID}.{DATASET_ID}.pipeline_monitoring`
        ORDER BY check_time DESC
        LIMIT 1
    """

    try:
        rows = list(bq_client.query(query).result())
        if not rows:
            logger.warning("PIPELINE_ALERT no monitoring rows found")
            return ({"status": "NO_ROWS"}, 200)

        status = rows[0].get("status")
        details = rows[0].get("details")

        if status == "ALERT":
            logger.error(f"PIPELINE_ALERT status=ALERT details={details}")
            return ({"status": "ALERT", "details": details}, 200)

        logger.info(f"PIPELINE_ALERT status=OK details={details}")
        return ({"status": "OK", "details": details}, 200)
    except Exception as exc:
        logger.error(f"PIPELINE_ALERT monitoring check failed: {exc}")
        return ({"status": "FAILED", "error": str(exc)}, 500)


# =============================================================================
# Local Testing Entry Point
# =============================================================================

def test_locally(bucket: str, file_name: str):
    """
    Test the function locally without Cloud Functions framework.
    
    Usage:
        python main.py --bucket ${GCS_BUCKET} --file logs/test-file.json
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
