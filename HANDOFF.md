**Important file references**
- Deployment script: [scripts/deploy.sh](scripts/deploy.sh#L1)
- Configuration: [config/settings.sh](config/settings.sh#L1)
- E2E test script: [scripts/e2e_gcp_test.sh](scripts/e2e_gcp_test.sh#L1)
- Cloud Function: [cloud_function/main.py](cloud_function/main.py#L1)
- SQL setup: [sql/00_setup_all.sql](sql/00_setup_all.sql#L1)
- Stored procedure: [sql/06_scheduled_query_proc.sql](sql/06_scheduled_query_proc.sql#L1)

**Required GCP APIs**
- BigQuery API
- Cloud Storage (GCS) API
- IAM API (for role bindings)
- (Optional) Cloud Logging API (for logs/alerts)
- (Optional) Cloud Functions API (if using Cloud Functions alternative future facing for realtime but long lived service discussed further in this doc)
- (Optional) Cloud Scheduler API (if deploying scheduled HTTP triggers)

**Required IAM roles — service account**
Grant these roles to the pipeline service account to allow all pipeline activities as deployed in `deploy.sh`:

- **BigQuery Data Editor** (`roles/bigquery.dataEditor`)
  - Allows inserting rows and creating/updating tables.
- **BigQuery Job User** (`roles/bigquery.jobUser`)
  - Allows running BigQuery queries including multi-statement SQL jobs.
- **Storage Object Viewer** on the GCS bucket bucket-level role
  - Command used in `deploy.sh`: `gsutil iam ch "serviceAccount:${SERVICE_ACCOUNT_EMAIL}:objectViewer" "gs://${GCS_BUCKET}"`

Notes and optional extras:
- If you will deploy the Cloud Function and have the function use the same service account, the SA must have the BigQuery and GCS roles above so runtime code in `cloud_function/main.py` can call BigQuery and read GCS.
- If deploying Cloud Functions via CI / automated deployment, the *deployer* needs additional roles such as `roles/cloudfunctions.developer` or `roles/editor` to create functions.

**Required IAM roles — human/deployer accounts**
The person or CI that runs `scripts/deploy.sh` should have:
- `roles/resourcemanager.projectIamAdmin` or sufficient project-level permissions to create service accounts and bind roles OR be an owner/editor with IAM privileges.
- `roles/bigquery.admin` or at least ability to create datasets/tables and run `bq` commands.
- `roles/storage.admin` or permission to create buckets and modify bucket ACLs or run `gsutil mb` and `gsutil iam ch`.
- `roles/iam.serviceAccountAdmin` to create service accounts (alternatively project owner).

**Configuration values the new team must set**
Edit or create `config/settings.sh` or a `settings.local.sh` next to it and ensure these are correct:
- **PROJECT_ID** — GCP project where resources live.
- **DATASET_ID** — BigQuery dataset name (default `logviewer`).
- **GCS_BUCKET** — e.g. `${PROJECT_ID}-ftplog` or custom bucket name.
- **GCS_LOGS_PREFIX** — default `logs` (folder/prefix in bucket).
- **SERVICE_ACCOUNT_NAME / SERVICE_ACCOUNT_EMAIL** — default `sa-logviewer@${PROJECT_ID}.iam.gserviceaccount.com`
- **BQ_LOCATION** — default `US` (used for dataset and transfers).
- **SCHEDULED_QUERY_NAME / schedule** — if you want to change schedule/tz.
- If you prefer not to commit secrets, place overrides in `config/settings.local.sh` (ignored by default).

**Deployment prerequisites**
- Billing enabled on the target project.
- gcloud CLI installed and authenticated use `gcloud auth login`.
- `bq` and `gsutil` CLIs installed and configured (deploy script checks them).
- The operator account has required deployer roles listed above.
- Ensure the APIs listed earlier are enabled for the project.

**Deployment steps (concise commands)**
1. Set project and confirm credentials:
```bash
gcloud auth login
gcloud config set project <PROJECT_ID>
```
2. Optionally populate `config/settings.local.sh`, or export env:
```bash
export PROJECT_ID=<YOUR_PROJECT>
# or edit config/settings.sh/settings.local.sh to set variables
```
3. Make deploy script executable and run:
```bash
cd <repo_root>/waystar-bq-load
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```
- This will: create the GCS bucket if missing, create the service account, grant roles, create BigQuery dataset & tables (via `sql/00_setup_all.sql` with token substitution), attempt to create scheduled query or instruct manual creation, upload sample file, and verify resources.

**E2E test — how to run**
- Ensure `PROJECT_ID` is exported or set in `config/settings.sh`.
- Run the end-to-end test:
```bash
export PROJECT_ID=<PROJECT>
bash scripts/e2e_gcp_test.sh
```
- What this script does:
  - Uploads `tests/sample_data/...` sample NDJSON to the GCS bucket.
  - Creates the stored procedure `proc_process_ftplog` using `sql/06_scheduled_query_proc.sql` (script substitutes `__PROJECT_ID__`, `__DATASET_ID__`, and `__GCS_URI__`).
  - Validates external table discovery (counts files/rows).
  - Executes the ETL (calls `CALL proc_process_ftplog()`).
  - Validates `processed_files` ledger, `base_ftplog` row count, and `archive_ftplog` row count.

**Expected E2E outcomes to validate assuming you are using the sample file with 5 rows**
- `processed_files` contains one row for the sample file with `status=SUCCESS` and `rows_loaded=5`.
- `base_ftplog` shows 5 newly inserted parsed rows.
- `archive_ftplog` shows 5 archived raw JSON rows.
- External table `external_ftplog_files` lists the sample file with `_FILE_NAME` and row_count 5.

**Key implementation details and gotchas the new team must know**
- SQL tokenization: SQL files use tokens `__PROJECT_ID__`, `__DATASET_ID__`, and `__GCS_URI__`. The scripts (`deploy.sh`, `e2e_gcp_test.sh`, `run_etl.sh`) perform `sed` substitution; do not run those SQL files raw without substitution.
- The combined setup script `sql/00_setup_all.sql` creates the external table using the GCS wildcard URI; ensure `GCS_BUCKET` and `GCS_LOGS_PREFIX` are correct before running.
- The stored procedure `proc_process_ftplog` is created by `sql/06_scheduled_query_proc.sql`. `deploy.sh` does not create it; `e2e_gcp_test.sh` does so remember to run the e2e script to create the procedure in a fresh deployment.
- Idempotency: `processed_files` ledger prevents duplicate processing; ETL checks existence and skips previously processed files.
- Cloud Function alternative: There is a near-real-time Cloud Function in `cloud_function/main.py`. If using it:
  - Deploy with `--service-account` set to the pipeline SA, or ensure function's runtime SA has the BigQuery/GCS roles.
  - If Cloud Scheduler will call HTTP functions, ensure the scheduler job has appropriate auth (OIDC) and the function has `roles/cloudfunctions.invoker` granted to the scheduler service identity or to the scheduler's service account.
- Monitoring & alerts: The `pipeline_monitoring` table is used for alert snapshots; alerts/scheduler jobs are configured under `monitoring/` and scripts/.

**Permissions breakdown (explicit)**
- Minimum SA permissions for runtime to operate without issues:
  - roles/bigquery.dataEditor
  - roles/bigquery.jobUser
  - Storage objectViewer on the bucket or `roles/storage.objectViewer` at bucket level
- Minimum deployer/automation permissions fro deployment and e2e test script:
  - Create service accounts & IAM bindings: `roles/iam.serviceAccountAdmin` or project owner
  - Create and modify buckets: `roles/storage.admin` or sufficient bucket permissions
  - Create BigQuery datasets/tables: `roles/bigquery.admin` or dataset-level create privileges
  - Create scheduled transfer/config: rights to use BigQuery Data Transfer. Transfer admin or bq CLI access
  - Deploy Cloud Functions if using: `roles/cloudfunctions.developer` + `roles/iam.serviceAccountUser` to allow assigning SA to function

**Troubleshooting checklist**
- If SQL jobs fail with invalid project/dataset IDs: confirm tokens were substituted; verify `PROJECT_ID` and `DATASET_ID` values in `config/settings.sh`.
- If `bq query` fails with permissions: ensure the calling identity deployer or SA used by function has `bigquery.jobUser`.
- If Cloud Function cannot access tables: confirm the function's runtime service account has BigQuery roles.
- If scheduled query doesn't run: check the transfer config UI in BigQuery and review transfer job logs and permissions.
- If GCS objects not discovered: confirm external table `external_ftplog_files` is created with the correct `uris` wildcard and `GCS_BUCKET/GCS_LOGS_PREFIX` are correct.
- Use `bq` to inspect table counts and runs:
  - `bq query --use_legacy_sql=false 'SELECT COUNT(*) FROM `<PROJECT>.<DATASET>.base_ftplog`'`
- Check Cloud Logging for function errors.