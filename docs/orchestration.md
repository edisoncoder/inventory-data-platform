# Orchestration v0.3.0

## Prefect Configuration

### Environment Variables (Required)
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=supply_chain_db
DB_USER=postgres
DB_PASSWORD=postgres

# v0.3.0 Watermark (optional)
WATERMARK_ENABLED=false           # Set 'true' for incremental load
CONTROL_SCHEMA=control             # Required if WATERMARK_ENABLED=true
WATERMARK_TABLE=watermarks

# Data paths
CSV_PATH=data/supply_chain_data.csv
RAW_SCHEMA=raw
RAW_TABLE=supply_chain_data
ANALYTICS_SCHEMA=analytics
ANALYTICS_TABLE=supply_chain_summary

# Prefect
PREFECT_API_URL=http://localhost:4200/api
```
### Start Prefect Server
```bash
prefect server start
# Access: http://127.0.0.1:4200
```

## Execution Methods
### Method 1: Manual Execution
```bash
python run_pipeline.py
```
```
# Output:
=== Starting Supply Chain Pipeline v0.3.0 ===
Watermark mode: DISABLED (v0.2.0 snapshot)
... [task logs] ...
=== Pipeline completed successfully! ===
```
### Method 2: Incremental Mode (v0.3.0)
```bash
export WATERMARK_ENABLED=true
python run_pipeline.py
```
#### First run (no watermark):
- Processes all 100 rows
- Sets watermark = 100
- Completes in ~120 seconds
#### Subsequent runs (watermark exists):
- Processes only rows with id > 100
- Updates watermark to new max_id
- Completes in ~30 seconds (if no new data) to ~60 seconds (if new data)
### Method 3: Cron Scheduling
```bash
crontab -e

# Add this line
0 2 * * * cd /path/to/inventory-data-platform && python run_pipeline.py >> /var/log/pipeline.log 2>&1
# Runs daily at 2 AM. Check logs: tail -f /var/log/pipeline.log
```
### Method 4: Prefect Deployment (Advanced)
```bash
# Build deployment
prefect deployment build prefect_pipeline.py:supply_chain_pipeline \
  -n supply-chain-prod \
  -q default

# Schedule run
prefect deployment run 'supply_chain_pipeline/supply-chain-prod'
```
## Watermark Mode
### Verify Watermark Status
```bash
# Check current watermark value
psql -U postgres -d supply_chain_db -c "SELECT * FROM control.watermarks;"

# Output:
# watermark_id | table_name         | last_processed_id | last_processed_timestamp
# 1            | supply_chain_data  | 100              | 2026-05-18 11:30:00
```
### Reset Watermark (Development Only)
```bash
# Reset to 0 (reprocess all rows on next run)
psql -U postgres -d supply_chain_db -c "UPDATE control.watermarks SET last_processed_id = 0 WHERE table_name = 'supply_chain_data';"
```
### Switch Modes
```bash
# From incremental to snapshot (v0.2.0)
export WATERMARK_ENABLED=false
python run_pipeline.py

# From snapshot to incremental (v0.3.0)
export WATERMARK_ENABLED=true
python run_pipeline.py
```
## Monitoring
### Prefect UI
1. Open http://127.0.0.1:4200
1. Click Flows → supply_chain_pipeline
1. View Recent Runs (status, duration, logs)
### Key metrics:
- Status: Completed, Failed, Running
- Duration: Total execution time
- Task Logs: Click run ID → expand task for detailed logs
### Console Logs
```bash
# Real-time output
python run_pipeline.py | tee pipeline_run.log

# Last 50 lines
tail -50 logs/pipeline_YYYY-MM-DD.log
```
## Performance Baselines
### Incremental Mode (v0.3.0):
- First run: 120 seconds (100% data)
- Subsequent runs: 30-40 seconds (delta only)
### Snapshot Mode (v0.2.0):
- Every run: 120 seconds (100% data)
## Troubleshooting
### Issue: Task quality_gate_incremental Fails
#### Error: ValueError: Null values found in 'id' column
**Cause:** CSV has null/missing values.
#### Solution:
1. Check CSV: head -20 data/supply_chain_data.csv
1. View detailed logs: grep quality_gate logs/pipeline_YYYY-MM-DD.log
1. Fix CSV data or increase null tolerance in quality_gate_incremental() task
### Issue: Watermark Not Updating
#### Error: UPDATE control.watermarks fails or watermark stays at 0
**Cause:**
- Control table doesn't exist
- CONTROL_SCHEMA not set in .env
- Permission denied on control schema
#### Solution:
```bash
# Verify control table exists
psql -U postgres -d supply_chain_db -c "\dt control.*"

# If missing, run migration
psql -U postgres -d supply_chain_db < sql/006_watermark_tracking.sql

# Verify CONTROL_SCHEMA is set
echo $CONTROL_SCHEMA
```
### Issue: "No new data to process"
**Status:** Normal. Pipeline continues to analytics layer.
**Meaning:** All rows in CSV have id <= last_watermark. No delta found.
**Next step:** Add new rows to CSV or reset watermark (dev only).
### Issue: Prefect Server Not Responding
**Error:** ConnectionError: Failed to reach API at http://127.0.0.1:4200/api/
#### Solution:
```bash
# Start Prefect server
prefect server start

# Wait 10 seconds for startup
sleep 10

# Retry pipeline
python run_pipeline.py
```
## Rollback Procedures
### Fallback to v0.2.0
```bash
# Disable incremental mode
export WATERMARK_ENABLED=false

# Run pipeline (snapshot mode)
python run_pipeline.py

# Verify: logs should show "Watermark mode: DISABLED"
```
### Revert Code to v0.2.0
```bash
git checkout v0.2.0
python run_pipeline.py
```
## SLA & Performance Targets
Metric | Target | Alert Threshold
-------- | -------- | --------
Duration (incremental, subsequent) | < 45 sec | > 2 min
Duration (snapshot) | < 120 sec | > 3 min
Data loss | 0% | > 0 = critical
Watermark sync delay | ±5 sec | > 1 min
Quality gate pass rate | > 99% | < 95%
## Advanced: Prefect Profiles
```bash
# List profiles
prefect profile ls

# Create profile for dev
prefect profile create dev
prefect config set PREFECT_HOME=~/.prefect-dev

# Use profile
prefect profile set-default dev
```
Operational guide. For architecture details, see docs/ARCHITECTURE.md. For tests, run pytest.