#!/usr/bin/env python3
"""
FTP Log Pipeline - Test Data Generator
=======================================
Generates realistic NDJSON test files for validating the BigQuery pipeline.

Features:
- Creates files matching the expected naming pattern
- Generates valid FTP log events with realistic data
- Supports configurable volume and date ranges
- Includes edge cases for testing (malformed JSON, edge timestamps)

Usage:
    python generate_test_data.py --output-dir ./test_files --num-files 3 --rows-per-file 100
    
    # Upload to GCS
    gsutil cp ./test_files/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import hashlib


# =============================================================================
# Configuration
# =============================================================================

# FTP server names that might appear in logs
FTP_SERVERS = [
    "FTP-SERVER-01",
    "FTP-SERVER-02", 
    "SFTP-PROD-01",
    "FTP-BACKUP-01"
]

# Possible FTP actions
FTP_ACTIONS = [
    "Store",      # Upload file
    "Retrieve",   # Download file
    "Login",      # User login
    "Logout",     # User logout
    "Delete",     # Delete file
    "Rename",     # Rename file
    "MakeDir",    # Create directory
    "RemoveDir",  # Remove directory
    "List",       # List directory
]

# Status codes and their responses (FTP standard codes)
STATUS_RESPONSES = {
    150: "Opening data connection.",
    200: "Command okay.",
    220: "Service ready for new user.",
    226: "Closing data connection. Requested file action successful.",
    227: "Entering Passive Mode.",
    230: "User logged in, proceed.",
    250: "Requested file action okay, completed.",
    257: "Directory created.",
    331: "User name okay, need password.",
    350: "Requested file action pending further information.",
    421: "Service not available, closing control connection.",
    425: "Can't open data connection.",
    426: "Connection closed; transfer aborted.",
    450: "Requested file action not taken.",
    500: "Syntax error, command unrecognized.",
    530: "Not logged in.",
    550: "Requested action not taken. File unavailable.",
}

# Sample customer IDs (numeric usernames)
SAMPLE_CUST_IDS = [12345, 67890, 11111, 22222, 33333, 44444, 55555]

# Sample partner names (non-numeric usernames)
SAMPLE_PARTNERS = ["partner_alpha", "partner_beta", "acme_corp", "globex_inc", "initech"]

# Sample file paths
SAMPLE_PATHS = [
    "/uploads/data_{}.txt",
    "/reports/report_{}.csv",
    "/exports/export_{}.xml",
    "/incoming/file_{}.dat",
    "/outgoing/batch_{}.json",
    "/archive/backup_{}.zip",
]

# Sample IP address ranges
IP_PREFIXES = ["192.168.1.", "10.0.0.", "172.16.0.", "203.0.113."]


# =============================================================================
# Data Generation Functions
# =============================================================================

def generate_ip_address() -> str:
    """Generate a random IP address."""
    prefix = random.choice(IP_PREFIXES)
    return f"{prefix}{random.randint(1, 254)}"


def generate_session_id() -> str:
    """Generate a random session ID."""
    return f"sess-{uuid.uuid4().hex[:12]}"


def compute_hash_code(data: str) -> int:
    """Compute a hash code similar to .NET GetHashCode()."""
    # Use MD5 and take first 8 hex chars as signed 32-bit int
    hash_bytes = hashlib.md5(data.encode()).hexdigest()[:8]
    value = int(hash_bytes, 16)
    # Convert to signed 32-bit integer
    if value >= 2**31:
        value -= 2**32
    return value


def generate_ftp_event(
    event_dt: datetime,
    source: str,
    is_customer: bool = True
) -> Dict:
    """Generate a single FTP log event."""
    
    # Determine user type
    if is_customer:
        cust_id = random.choice(SAMPLE_CUST_IDS)
        user_name = str(cust_id)
        partner_name = None
    else:
        cust_id = 0
        partner_name = random.choice(SAMPLE_PARTNERS)
        user_name = partner_name
    
    # Choose action and appropriate status
    action = random.choice(FTP_ACTIONS)
    
    # Weight status codes based on action (most operations succeed)
    if random.random() < 0.9:  # 90% success
        if action == "Login":
            status_code = 230
        elif action == "Logout":
            status_code = 221 if 221 in STATUS_RESPONSES else 226
        elif action in ["Store", "Retrieve"]:
            status_code = 226
        elif action in ["Delete", "Rename", "MakeDir", "RemoveDir"]:
            status_code = 250
        else:
            status_code = 200
    else:  # 10% failures
        status_code = random.choice([421, 425, 426, 450, 530, 550])
    
    server_response = STATUS_RESPONSES.get(status_code, "Unknown status")
    
    # Generate filename
    if action in ["Store", "Retrieve", "Delete", "Rename"]:
        path_template = random.choice(SAMPLE_PATHS)
        filename = path_template.format(random.randint(1000, 9999))
        bytes_transferred = random.randint(1024, 10_000_000) if action in ["Store", "Retrieve"] else 0
    else:
        filename = "-"
        bytes_transferred = 0
    
    # Generate raw data line
    raw_data = (
        f"{event_dt.strftime('%Y-%m-%d %H:%M:%S')} "
        f"{generate_ip_address()} "
        f"{user_name} "
        f"{action.upper()} "
        f"{filename} "
        f"{status_code} "
        f"{bytes_transferred}"
    )
    
    event = {
        "UserName": user_name,
        "CustId": cust_id,
        "PartnerName": partner_name,
        "EventDt": event_dt.strftime("%Y-%m-%dT%H:%M:%S"),
        "Action": action,
        "Filename": filename,
        "SessionId": generate_session_id(),
        "IpAddress": generate_ip_address(),
        "Source": source,
        "Bytes": bytes_transferred,
        "StatusCode": status_code,
        "ServerResponse": server_response,
        "RawData": raw_data,
        "HashCode": compute_hash_code(raw_data),
    }
    
    return event


def generate_ftplog_file(
    base_timestamp: datetime,
    source: str,
    num_events: int,
    include_malformed: bool = False
) -> tuple[str, List[str]]:
    """
    Generate a complete NDJSON file with FTP log events.
    
    Returns:
        Tuple of (filename, list of JSON lines)
    """
    # Generate filename matching the .NET pattern
    # {Source}-{EventDt:yyyyMMdd-HHmmss}{Now:ffff}-{Guid}
    timestamp_str = base_timestamp.strftime("%Y%m%d-%H%M%S")
    microseconds = f"{base_timestamp.microsecond:04d}"[:4]
    guid = str(uuid.uuid4())
    filename = f"{source}-{timestamp_str}{microseconds}-{guid}.json"
    
    lines = []
    
    # Generate events spread across a time window
    for i in range(num_events):
        # Spread events across 5-minute window
        event_offset = timedelta(seconds=random.randint(0, 300))
        event_dt = base_timestamp + event_offset
        
        # Mix of customers (70%) and partners (30%)
        is_customer = random.random() < 0.7
        
        event = generate_ftp_event(event_dt, source, is_customer)
        lines.append(json.dumps(event, separators=(',', ':')))
    
    # Optionally include malformed JSON for testing error handling
    if include_malformed:
        malformed_lines = [
            '{"incomplete": "json',  # Missing closing brace
            'not json at all',       # Plain text
            '',                      # Empty line
            '   ',                   # Whitespace only
        ]
        # Insert malformed lines at random positions
        for malformed in malformed_lines:
            if random.random() < 0.5:  # 50% chance to include each
                pos = random.randint(0, len(lines))
                lines.insert(pos, malformed)
    
    return filename, lines


def generate_test_files(
    output_dir: str,
    num_files: int,
    rows_per_file: int,
    start_date: Optional[datetime] = None,
    include_malformed: bool = False
) -> List[str]:
    """
    Generate multiple test NDJSON files.
    
    Args:
        output_dir: Directory to write files to
        num_files: Number of files to generate
        rows_per_file: Approximate number of events per file
        start_date: Starting timestamp (defaults to now)
        include_malformed: Whether to include malformed JSON lines
        
    Returns:
        List of generated file paths
    """
    os.makedirs(output_dir, exist_ok=True)
    
    if start_date is None:
        start_date = datetime.now()
    
    generated_files = []
    
    for i in range(num_files):
        # Each file represents a 5-minute batch, offset by file index
        file_timestamp = start_date - timedelta(minutes=5 * i)
        source = random.choice(FTP_SERVERS)
        
        # Vary the number of rows slightly
        actual_rows = rows_per_file + random.randint(-rows_per_file // 10, rows_per_file // 10)
        actual_rows = max(1, actual_rows)
        
        filename, lines = generate_ftplog_file(
            base_timestamp=file_timestamp,
            source=source,
            num_events=actual_rows,
            include_malformed=include_malformed and (i == num_files - 1)  # Only last file has malformed
        )
        
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        
        generated_files.append(filepath)
        print(f"Generated: {filename} ({len(lines)} lines)")
    
    return generated_files


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate test NDJSON files for FTP Log Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 3 files with 100 rows each
  python generate_test_data.py --output-dir ./test_files --num-files 3 --rows-per-file 100
  
  # Generate files with malformed JSON for error testing
  python generate_test_data.py --output-dir ./test_files --num-files 2 --rows-per-file 50 --include-malformed
  
  # Generate files with specific start date
  python generate_test_data.py --output-dir ./test_files --num-files 5 --start-date "2026-01-28T10:00:00"
  
After generating, upload to GCS:
  gsutil cp ./test_files/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/
        """
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        default='./test_files',
        help='Directory to write generated files (default: ./test_files)'
    )
    parser.add_argument(
        '--num-files', '-n',
        type=int,
        default=3,
        help='Number of files to generate (default: 3)'
    )
    parser.add_argument(
        '--rows-per-file', '-r',
        type=int,
        default=100,
        help='Approximate number of events per file (default: 100)'
    )
    parser.add_argument(
        '--start-date', '-d',
        type=str,
        default=None,
        help='Start date in ISO format (default: now)'
    )
    parser.add_argument(
        '--include-malformed', '-m',
        action='store_true',
        help='Include malformed JSON lines in last file for error testing'
    )
    parser.add_argument(
        '--seed', '-s',
        type=int,
        default=None,
        help='Random seed for reproducible output'
    )
    
    args = parser.parse_args()
    
    # Set random seed if provided
    if args.seed is not None:
        random.seed(args.seed)
        print(f"Using random seed: {args.seed}")
    
    # Parse start date if provided
    start_date = None
    if args.start_date:
        start_date = datetime.fromisoformat(args.start_date)
    
    print(f"\n{'='*60}")
    print("FTP Log Pipeline - Test Data Generator")
    print(f"{'='*60}")
    print(f"Output directory: {args.output_dir}")
    print(f"Number of files: {args.num_files}")
    print(f"Rows per file: ~{args.rows_per_file}")
    print(f"Include malformed: {args.include_malformed}")
    print(f"{'='*60}\n")
    
    # Generate files
    generated_files = generate_test_files(
        output_dir=args.output_dir,
        num_files=args.num_files,
        rows_per_file=args.rows_per_file,
        start_date=start_date,
        include_malformed=args.include_malformed
    )
    
    print(f"\n{'='*60}")
    print(f"Generated {len(generated_files)} files")
    print(f"{'='*60}")
    print("\nNext steps:")
    print(f"  1. Review files: ls -la {args.output_dir}")
    print(f"  2. View sample: head -5 {generated_files[0] if generated_files else '<file>'}")
    print(f"  3. Upload to GCS:")
    print(f"     gsutil cp {args.output_dir}/*.json gs://sbox-ravelar-001-20250926-ftplog/logs/")
    print()


if __name__ == '__main__':
    main()
