# Architecture

## Overview

This pipeline processes FTP log events stored as NDJSON files in Google Cloud Storage and loads them into BigQuery for analysis.

## Data Flow

```
.NET Service → GCS Bucket → BigQuery ETL → Structured Tables
                  │                              │
                  │    ┌────────────────┐       │
                  └───→│ External Table │───────┘
                       └────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
        base_ftplog    archive_ftplog   processed_files
        (Structured)   (Raw JSON)       (Tracking)
```

## Components

### BigQuery Tables

| Table | Purpose |
|-------|---------|
| `base_ftplog` | Structured, queryable FTP events (partitioned by `event_dt`, clustered by `source`, `cust_id`) |
| `archive_ftplog` | Raw JSON archive for compliance (partitioned by `archived_timestamp`) |
| `processed_files` | Tracks which GCS files have been processed (idempotency ledger) |
| `external_ftplog_files` | External table pointing to GCS (no data storage) |
| `pipeline_monitoring` | Point-in-time health snapshots for alerting |

### Processing Options

| Option | Latency | When to Use |
|--------|---------|-------------|
| Scheduled Query (Recommended) | 5-15 min | Standard use case |
| Cloud Function (Alternative) | <1 min | Real-time requirements |

### ETL Logic

The ETL stored procedure (`proc_process_ftplog`) performs these steps:

1. **Identify new files**: Query `external_ftplog_files` for files not in `processed_files`
2. **Parse and insert**: Use `JSON_VALUE` to extract fields, insert into `base_ftplog`
3. **Archive**: Store raw JSON lines in `archive_ftplog`
4. **Mark processed**: Insert into `processed_files` with status and row counts

### Idempotency

- Files are tracked by GCS URI in `processed_files`
- Rows are deduplicated using `hash_fingerprint` (SHA256 of canonical fields)
- Re-running ETL on already-processed files is a no-op

### Source Data Format

Files must be NDJSON with this schema:

```json
{
  "UserName": "12345",
  "CustId": 12345,
  "PartnerName": null,
  "EventDt": "2026-01-28T10:30:00.000Z",
  "Action": "Store",
  "Filename": "/uploads/data.txt",
  "SessionId": "sess-abc123",
  "IpAddress": "192.168.1.100",
  "Source": "FTP-SERVER-01",
  "Bytes": 2048,
  "StatusCode": 226,
  "ServerResponse": "Closing data connection.",
  "RawData": "...",
  "HashCode": -1680792964
}
```

**Note**: `StatusCode` is intentionally NOT loaded to base table (per Snowflake parity requirement).
