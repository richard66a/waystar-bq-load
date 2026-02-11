# PR Checklist

## Pre-merge
- [ ] `config/settings.sh` is local-only or sanitized; no environment secrets committed.
- [ ] `docs/CHANGELOG.md` updated with user-facing changes.
- [ ] Scripts are executable and run with `set -euo pipefail`.
- [ ] Python files pass a syntax check (e.g., `python3 -m py_compile`).
- [ ] Test data uses UTC `EventDt` with trailing `Z`.
- [ ] `hash_code` is deterministic in test data and test harness.
- [ ] Docs link to canonical content in `docs/`.

## Post-merge validation
- [ ] Run the E2E script and confirm `processed_files.status = SUCCESS`.
- [ ] Confirm `rows_expected == rows_loaded` for the newest file.
- [ ] Verify base/archive row parity for the newest file.
