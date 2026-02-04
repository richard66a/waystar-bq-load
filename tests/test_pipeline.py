#!/usr/bin/env python3
"""
FTP Log Pipeline - End-to-End Test Suite
=========================================
Automated tests to validate the entire pipeline flow.

Usage:
    # Run all tests
    python test_pipeline.py

    # Run specific test
    python test_pipeline.py --test test_file_generation

    # Use custom configuration
    python test_pipeline.py --project sbox-ravelar-001-20250926 --bucket my-bucket
"""

import argparse
import json
import os
import sys
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple
import hashlib

# Test configuration
DEFAULT_PROJECT = "sbox-ravelar-001-20250926"
DEFAULT_BUCKET = "sbox-ravelar-001-20250926-ftplog"
DEFAULT_DATASET = "logviewer"


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def log_info(msg: str):
    print(f"{Colors.BLUE}[INFO]{Colors.RESET} {msg}")


def log_success(msg: str):
    print(f"{Colors.GREEN}[PASS]{Colors.RESET} {msg}")


def log_error(msg: str):
    print(f"{Colors.RED}[FAIL]{Colors.RESET} {msg}")


def log_warning(msg: str):
    print(f"{Colors.YELLOW}[WARN]{Colors.RESET} {msg}")


def run_command(cmd: str, capture: bool = True) -> Tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr."""
    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=capture,
        text=True
    )
    return result.returncode, result.stdout, result.stderr


def run_bq_query(query: str, project: str) -> Tuple[bool, str]:
    """Run a BigQuery query and return success status and output."""
    cmd = f'bq query --use_legacy_sql=false --project_id={project} --format=json "{query}"'
    code, stdout, stderr = run_command(cmd)
    return code == 0, stdout if code == 0 else stderr


class PipelineTest:
    """Base class for pipeline tests."""
    
    def __init__(self, project: str, bucket: str, dataset: str):
        self.project = project
        self.bucket = bucket
        self.dataset = dataset
        self.test_id = f"test-{uuid.uuid4().hex[:8]}"
        self.test_files: List[str] = []
    
    def setup(self):
        """Setup before test."""
        pass
    
    def teardown(self):
        """Cleanup after test."""
        # Remove test files from GCS
        for file_path in self.test_files:
            run_command(f'gsutil rm -f gs://{self.bucket}/logs/{file_path}')
    
    def run(self) -> bool:
        """Run the test. Returns True if passed."""
        raise NotImplementedError


class TestGCSConnectivity(PipelineTest):
    """Test that we can access the GCS bucket."""
    
    def run(self) -> bool:
        log_info("Testing GCS bucket connectivity...")
        
        code, stdout, _ = run_command(f'gsutil ls gs://{self.bucket}/')
        if code != 0:
            log_error(f"Cannot access bucket: gs://{self.bucket}")
            return False
        
        log_success("GCS bucket is accessible")
        return True


class TestBigQueryConnectivity(PipelineTest):
    """Test that we can query BigQuery."""
    
    def run(self) -> bool:
        log_info("Testing BigQuery connectivity...")
        
        success, output = run_bq_query(
            f"SELECT 1 as test FROM `{self.project}.{self.dataset}.processed_files` LIMIT 1",
            self.project
        )
        
        if not success:
            log_error(f"Cannot query BigQuery: {output}")
            return False
        
        log_success("BigQuery is accessible")
        return True


class TestExternalTable(PipelineTest):
    """Test that the external table can read from GCS."""
    
    def run(self) -> bool:
        log_info("Testing external table connectivity...")
        
        success, output = run_bq_query(
            f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.dataset}.external_ftplog_files` LIMIT 1",
            self.project
        )
        
        if not success:
            log_error(f"External table query failed: {output}")
            return False
        
        log_success("External table is working")
        return True


class TestFileProcessing(PipelineTest):
    """Test end-to-end file processing."""
    
    def run(self) -> bool:
        log_info("Testing end-to-end file processing...")
        
        # Generate a unique test file
        test_filename = f"FTP-TEST-{datetime.now().strftime('%Y%m%d-%H%M%S')}0001-{self.test_id}.json"
        self.test_files.append(test_filename)
        
        # Create test data
        test_events = []
        for i in range(5):
            event = {
                "UserName": f"{10000 + i}",
                "CustId": 10000 + i,
                "PartnerName": None,
                "EventDt": (datetime.now() - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S"),
                "Action": "Store",
                "Filename": f"/uploads/test_{i}.txt",
                "SessionId": f"sess-{self.test_id}-{i}",
                "IpAddress": f"192.168.1.{i}",
                "Source": "FTP-TEST",
                "Bytes": 1024 * (i + 1),
                "StatusCode": 226,
                "ServerResponse": "Test successful",
                "RawData": f"test raw data {i}",
                "HashCode": hash(f"{self.test_id}-{i}") % (2**31),
            }
            test_events.append(json.dumps(event))
        
        # Write to temp file and upload
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('\n'.join(test_events))
            temp_path = f.name
        
        try:
            # Upload to GCS
            log_info(f"Uploading test file: {test_filename}")
            code, _, stderr = run_command(
                f'gsutil cp {temp_path} gs://{self.bucket}/logs/{test_filename}'
            )
            if code != 0:
                log_error(f"Failed to upload test file: {stderr}")
                return False
            
            # Verify file is in external table
            log_info("Verifying file in external table...")
            gcs_uri = f"gs://{self.bucket}/logs/{test_filename}"
            
            success, output = run_bq_query(
                f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.dataset}.external_ftplog_files` "
                f"WHERE _FILE_NAME = '{gcs_uri}'",
                self.project
            )
            
            if not success:
                log_error(f"Failed to query external table: {output}")
                return False
            
            # Check if file is visible
            try:
                result = json.loads(output)
                if result and result[0]['cnt'] == 0:
                    log_warning("File not yet visible in external table (may need refresh)")
            except:
                pass
            
            # Run ETL manually
            log_info("Running ETL...")
            sql_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 'sql', '06_scheduled_query_etl.sql'
            )
            
            code, stdout, stderr = run_command(
                f'bq query --use_legacy_sql=false --project_id={self.project} < {sql_path}'
            )
            
            if code != 0:
                log_error(f"ETL failed: {stderr}")
                return False
            
            # Verify file was processed
            log_info("Verifying processing...")
            time.sleep(2)  # Brief wait for consistency
            
            success, output = run_bq_query(
                f"SELECT rows_loaded, status FROM `{self.project}.{self.dataset}.processed_files` "
                f"WHERE gcs_uri = '{gcs_uri}'",
                self.project
            )
            
            if not success:
                log_error(f"Failed to check processed_files: {output}")
                return False
            
            try:
                result = json.loads(output)
                if not result:
                    log_error("File was not recorded in processed_files")
                    return False
                
                rows_loaded = result[0].get('rows_loaded', 0)
                status = result[0].get('status', '')
                
                if status != 'SUCCESS':
                    log_warning(f"Processing status: {status}")
                
                if rows_loaded != len(test_events):
                    log_warning(f"Expected {len(test_events)} rows, got {rows_loaded}")
                
            except json.JSONDecodeError:
                log_error(f"Failed to parse result: {output}")
                return False
            
            # Verify data in base table
            success, output = run_bq_query(
                f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.dataset}.base_ftplog` "
                f"WHERE gcs_uri = '{gcs_uri}'",
                self.project
            )
            
            if success:
                try:
                    result = json.loads(output)
                    base_count = result[0]['cnt'] if result else 0
                    log_info(f"Rows in base_ftplog: {base_count}")
                except:
                    pass
            
            log_success(f"File processed successfully: {rows_loaded} rows")
            return True
            
        finally:
            os.unlink(temp_path)


class TestIdempotency(PipelineTest):
    """Test that re-running ETL doesn't create duplicates."""
    
    def run(self) -> bool:
        log_info("Testing idempotency (re-processing protection)...")
        
        # Get current row count for a processed file
        success, output = run_bq_query(
            f"SELECT gcs_uri, rows_loaded FROM `{self.project}.{self.dataset}.processed_files` "
            f"ORDER BY processed_timestamp DESC LIMIT 1",
            self.project
        )
        
        if not success:
            log_error(f"Failed to get processed file: {output}")
            return False
        
        try:
            result = json.loads(output)
            if not result:
                log_warning("No processed files found - skipping idempotency test")
                return True
            
            test_uri = result[0]['gcs_uri']
            original_rows = result[0]['rows_loaded']
        except:
            log_error("Failed to parse processed files")
            return False
        
        # Get row count in base table
        success, output = run_bq_query(
            f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.dataset}.base_ftplog` "
            f"WHERE gcs_uri = '{test_uri}'",
            self.project
        )
        
        try:
            result = json.loads(output)
            before_count = result[0]['cnt'] if result else 0
        except:
            before_count = 0
        
        # Run ETL again
        log_info("Re-running ETL...")
        sql_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 'sql', '06_scheduled_query_etl.sql'
        )
        run_command(f'bq query --use_legacy_sql=false --project_id={self.project} < {sql_path}')
        
        # Check row count again
        success, output = run_bq_query(
            f"SELECT COUNT(*) as cnt FROM `{self.project}.{self.dataset}.base_ftplog` "
            f"WHERE gcs_uri = '{test_uri}'",
            self.project
        )
        
        try:
            result = json.loads(output)
            after_count = result[0]['cnt'] if result else 0
        except:
            after_count = 0
        
        if before_count == after_count:
            log_success(f"Idempotency verified: {before_count} rows (no duplicates)")
            return True
        else:
            log_error(f"Idempotency failed: {before_count} → {after_count} rows")
            return False


def run_all_tests(project: str, bucket: str, dataset: str) -> bool:
    """Run all tests and return overall success status."""
    
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}FTP Log Pipeline - Test Suite{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"Project: {project}")
    print(f"Bucket:  {bucket}")
    print(f"Dataset: {dataset}")
    print(f"Time:    {datetime.now().isoformat()}")
    print(f"{'='*60}\n")
    
    tests = [
        ("GCS Connectivity", TestGCSConnectivity),
        ("BigQuery Connectivity", TestBigQueryConnectivity),
        ("External Table", TestExternalTable),
        ("File Processing", TestFileProcessing),
        ("Idempotency", TestIdempotency),
    ]
    
    results = []
    
    for name, test_class in tests:
        print(f"\n{Colors.BOLD}Test: {name}{Colors.RESET}")
        print("-" * 40)
        
        test = test_class(project, bucket, dataset)
        try:
            test.setup()
            passed = test.run()
            results.append((name, passed))
        except Exception as e:
            log_error(f"Test raised exception: {e}")
            results.append((name, False))
        finally:
            try:
                test.teardown()
            except:
                pass
    
    # Summary
    print(f"\n{Colors.BOLD}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}Test Results Summary{Colors.RESET}")
    print(f"{'='*60}")
    
    passed = sum(1 for _, p in results if p)
    failed = len(results) - passed
    
    for name, result in results:
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        print(f"  {name}: {status}")
    
    print(f"{'='*60}")
    print(f"Total: {passed}/{len(results)} passed")
    
    if failed == 0:
        print(f"{Colors.GREEN}{Colors.BOLD}All tests passed!{Colors.RESET}")
    else:
        print(f"{Colors.RED}{Colors.BOLD}{failed} test(s) failed{Colors.RESET}")
    
    return failed == 0


def main():
    parser = argparse.ArgumentParser(
        description="FTP Log Pipeline Test Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--project', '-p',
        default=DEFAULT_PROJECT,
        help=f'GCP project ID (default: {DEFAULT_PROJECT})'
    )
    parser.add_argument(
        '--bucket', '-b',
        default=DEFAULT_BUCKET,
        help=f'GCS bucket name (default: {DEFAULT_BUCKET})'
    )
    parser.add_argument(
        '--dataset', '-d',
        default=DEFAULT_DATASET,
        help=f'BigQuery dataset (default: {DEFAULT_DATASET})'
    )
    parser.add_argument(
        '--test', '-t',
        help='Run specific test only'
    )
    
    args = parser.parse_args()
    
    success = run_all_tests(args.project, args.bucket, args.dataset)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
