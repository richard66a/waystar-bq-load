#!/usr/bin/env python3
"""
Validate local NDJSON sample files for the ftplog pipeline.

Checks per-line JSON parseability and key/type expectations that mirror
the ETL's JSON_VALUE / SAFE_CAST assumptions.
"""
import argparse
import json
from datetime import datetime
from pathlib import Path
import sys


REQUIRED_KEYS = [
    "UserName",
    "CustId",
    "EventDt",
    "Action",
    "Filename",
    "SessionId",
    "IpAddress",
    "Source",
    "Bytes",
    "StatusCode",
]


def parse_eventdt(v: str):
    # Our ETL uses SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', ...)
    # Python's fromisoformat handles YYYY-MM-DDTHH:MM:SS and fractional seconds.
    return datetime.fromisoformat(v)


def validate_file(path: Path):
    summary = {"file": str(path), "lines": 0, "ok": 0, "errors": 0, "examples": []}
    with path.open("r", encoding="utf-8") as f:
        for i, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            summary["lines"] += 1
            try:
                obj = json.loads(raw)
            except Exception as e:
                summary["errors"] += 1
                if len(summary["examples"]) < 3:
                    summary["examples"].append({"line": i, "error": f"json:{e}"})
                continue

            missing = [k for k in REQUIRED_KEYS if k not in obj]
            if missing:
                summary["errors"] += 1
                if len(summary["examples"]) < 3:
                    summary["examples"].append({"line": i, "error": f"missing:{missing}"})
                continue

            # Type checks
            try:
                int(obj.get("CustId"))
                int(obj.get("Bytes", 0))
                int(obj.get("StatusCode", 0))
            except Exception as e:
                summary["errors"] += 1
                if len(summary["examples"]) < 3:
                    summary["examples"].append({"line": i, "error": f"int_cast:{e}"})
                continue

            # Timestamp parse
            try:
                _ = parse_eventdt(obj.get("EventDt"))
            except Exception as e:
                summary["errors"] += 1
                if len(summary["examples"]) < 3:
                    summary["examples"].append({"line": i, "error": f"ts:{e}"})
                continue

            summary["ok"] += 1
            if len(summary["examples"]) < 3:
                # store a small projection
                summary["examples"].append({
                    "line": i,
                    "cust_id": int(obj.get("CustId")),
                    "event_dt": obj.get("EventDt"),
                    "filename": obj.get("Filename"),
                })

    return summary


def find_samples(base: Path):
    # Look for Waystar-*.json in base and top-level NDJSON files
    candidates = list(base.glob("Waystar-*.json")) + list(base.glob("*.ndjson")) + list(base.glob("*.json"))
    # de-duplicate preserving order
    seen = set()
    out = []
    for p in candidates:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="*", help="NDJSON files to validate (defaults to auto-discovery)")
    args = ap.parse_args()

    base = Path.cwd()
    if args.files:
        paths = [Path(p) for p in args.files]
    else:
        paths = find_samples(base)

    if not paths:
        print("No sample files found in current directory. Provide paths on the CLI.")
        sys.exit(2)

    results = []
    total_lines = total_ok = total_errors = 0
    for p in paths:
        if not p.exists():
            print(f"Skipping missing file: {p}")
            continue
        s = validate_file(p)
        results.append(s)
        total_lines += s["lines"]
        total_ok += s["ok"]
        total_errors += s["errors"]

    print("\nValidation Summary")
    print("------------------")
    for r in results:
        print(f"{r['file']}: lines={r['lines']} ok={r['ok']} errors={r['errors']}")
        if r["examples"]:
            print("  examples:")
            for e in r["examples"]:
                print(f"    {e}")

    print(f"\nTotal: lines={total_lines} ok={total_ok} errors={total_errors}")
    if total_errors:
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()
