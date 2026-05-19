# Inventory Data Platform v0.3.0

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/Prefect-2.x-orange?logo=prefect&logoColor=white)](https://www.prefect.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-lightblue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![pytest](https://img.shields.io/badge/pytest-7.x-green?logo=pytest&logoColor=white)](https://pytest.org/)

## Overview

A modern, production-grade ETL (Extract, Transform, Load) pipeline built with **Prefect 2.x** orchestration and **PostgreSQL**. Version 0.3.0 introduces **incremental load** with watermark-based checkpointing, reducing execution time by 70-80% while maintaining ACID compliance and full idempotency.

### Key Features

- **Incremental Load**: Process only new/changed data via watermark checkpoint
- **Dual Modes**: Incremental (v0.3.0) or snapshot (v0.2.0 legacy, default)
- **ACID Compliance**: All database operations wrapped in atomic transactions
- **Quality Gates**: Automated validation before data ingestion (fail-fast)
- **Prefect Orchestration**: State management, retries, monitoring via UI
- **Comprehensive Tests**: 13 unit tests with mocking and fixtures
- **Production-Ready**: Logging, alerting, error recovery

## Architecture

```
CSV Source
    ↓
[Get Watermark] ← checkpoint (last processed ID)
    ↓
[Load Incremental Data] → filter: id > watermark
    ↓
[Quality Gate] → validate schema, nulls, duplicates
    ↓
[Ingest to Raw] → ACID transaction (append)
    ↓
[Update Watermark] → persist new checkpoint
    ↓
[Build Analytics] → transform and aggregate
    ↓
PostgreSQL (Raw + Analytics Layers)
```

**Backward Compatible**: Set `WATERMARK_ENABLED=false` to use v0.2.0 snapshot mode (full refresh).

## Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.9+ | Execution runtime |
| Prefect | 2.x | Workflow orchestration |
| PostgreSQL | 15+ | Data persistence (ACID) |
| SQLAlchemy | 2.x | ORM + connection pooling |
| Pandas | 2.x | Data manipulation |
| Pydantic | 2.x | Data validation |
| pytest | 7.x | Unit testing |
| Docker | Latest | Environment isolation |

## Prerequisites

- **Python 3.9+**
- **PostgreSQL 15+** (running instance)
- **Docker & Docker Compose** (optional, for full stack)
- **Prefect Server** (local or cloud)

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/edisoncoder/inventory-data-platform.git
cd inventory-data-platform
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials and v0.3.0 settings
```

Key environment variables:

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=inventory_db
DB_USER=inventory_user
DB_PASSWORD=inventory_pass

# v0.3.0: Incremental Load (optional, default: false)
WATERMARK_ENABLED=true
CONTROL_SCHEMA=control

# Data paths
CSV_PATH=data/supply_chain_data.csv
RAW_SCHEMA=raw
RAW_TABLE=supply_chain_data
ANALYTICS_SCHEMA=analytics
ANALYTICS_TABLE=supply_chain_summary
```

### 5. Initialize Database

```bash
# Start PostgreSQL (via docker-compose or local instance)
docker-compose up -d postgres

# Run migrations
psql -U inventory_user -d inventory_db < sql/001_init.sql
# ... 
psql -U inventory_user -d inventory_db < sql/006_watermark_tracking.sql  # v0.3.0
```

### 6. Start Prefect Server (Optional)

```bash
prefect server start
# Access UI: http://localhost:4200
```

## Execution

### Mode 1: Manual Execution

```bash
python run_pipeline.py

# Expected output:
# === Starting Supply Chain Pipeline v0.3.0 ===
# Watermark mode: ENABLED (or DISABLED for v0.2.0)
# ... [task logs] ...
# === Pipeline completed successfully! ===
```

### Mode 2: Incremental Load (v0.3.0)

```bash
export WATERMARK_ENABLED=true
python run_pipeline.py

# First run: processes all 100 rows, sets watermark=100
# Second run: processes only new rows (if CSV updated)
# Subsequent runs: uses watermark checkpoint for delta processing
```

### Mode 3: Snapshot Mode (v0.2.0 Legacy)

```bash
export WATERMARK_ENABLED=false
python run_pipeline.py

# Processes all rows every run (full refresh)
# No watermark table required
```

### Mode 4: Scheduled via Cron

```bash
crontab -e

# Add:
0 2 * * * cd /path/to/project && python run_pipeline.py

# Runs daily at 2 AM
```

## Monitoring

### Prefect UI

1. Start Prefect server: `prefect server start`
2. Open http://localhost:4200
3. Navigate to Flows → supply_chain_pipeline
4. View Recent Runs: status, duration, logs per task

### Logs

- **Console**: Real-time output from `python run_pipeline.py`
- **File**: `logs/pipeline_YYYY-MM-DD.log` (daily rotation)
- **Task Logs**: Click run ID in Prefect UI → drill into task logs

### Performance Metrics

**Incremental Mode (v0.3.0):**
- First run: ~120 seconds (processes 100% of data, sets watermark)
- 2nd+ runs: ~30-40 seconds (processes only new rows)

**Snapshot Mode (v0.2.0):**
- Every run: ~120 seconds (full refresh, no watermark)

## Testing

### Run All Tests

```bash
pytest tests/ -v --cov=src --cov-report=html
```

Expected output:

```
tests/test_incremental_load.py::TestGetWatermark::test_get_watermark_first_run PASSED
tests/test_incremental_load.py::TestGetWatermark::test_get_watermark_existing PASSED
tests/test_incremental_load.py::TestLoadIncrementalData::test_load_incremental_filters_correctly PASSED
... [9 more tests] ...
===== 13 passed in 0.45s =====
Coverage: 85%+
```

### Run Specific Test Class

```bash
pytest tests/test_incremental_load.py::TestQualityGateIncremental -v
```

### Generate Coverage Report

```bash
pytest tests/ --cov=src --cov-report=html
open htmlcov/index.html  # View in browser
```

## File Structure

```
inventory-data-platform/
├── .env                          # Environment variables (local)
├── .env.example                  # Template
├── README.md                     # This file
├── CHANGELOG.md                  # Version history
├── requirements.txt              # Python dependencies
├── docker-compose.yml            # PostgreSQL + Adminer
│
├── src/
│   └── config.py                 # Configuration loader (get_config)
│
├── sql/
│   ├── 001_init.sql              # Initial schema (raw, analytics)
│   ├── 002_analytics_tables.sql  # Analytics views
│   ├── 003_pipeline_log.sql      # Logging table
│   ├── 004_dimensional_model.sql # Dimensional tables
│   ├── 005_staging_layer.sql     # Staging transformations
│   └── 006_watermark_tracking.sql# v0.3.0: Watermark control table
│
├── tests/
│   └── test_incremental_load.py  # 13 unit tests (pytest)
│
├── prefect_pipeline.py           # Main Prefect flow (v0.3.0)
├── run_pipeline.py               # Manual execution entry point
├── run_pipeline.sh               # Shell wrapper (legacy cron)
│
├── data/
│   └── supply_chain_data.csv     # Sample input data (100 rows)
│
├── docs/
│   ├── ARCHITECTURE.md           # Technical architecture
│   ├── DATA_LAYERS.md            # Raw/staging/analytics explanation
│   ├── OBSERVABILITY.md          # Logging, alerts, metrics
│   ├── ORCHESTRATION.md          # Prefect guide (operational)
│   └── ROADMAP.md                # v0.3.0 → v1.0.0+ evolution
│
├── logs/                         # Pipeline execution logs (daily)
└── alerts/                       # Alert logs on failure
```

## Version Comparison

| Feature | v0.1.0 | v0.2.0 | v0.3.0 |
|---------|--------|--------|--------|
| CSV Ingestion | ✅ | ✅ | ✅ |
| PostgreSQL | ✅ | ✅ | ✅ |
| Quality Gates | ✅ | ✅ | ✅ (Enhanced) |
| Prefect Orchestration | ❌ | ✅ | ✅ |
| ACID Compliance | ❌ | ✅ | ✅ |
| Incremental Load | ❌ | ❌ | ✅ |
| Watermark Checkpoint | ❌ | ❌ | ✅ |
| Unit Tests | ❌ | ❌ | ✅ (13 tests) |

## Roadmap

**v0.3.0** ✅ (Current)
- Incremental load with watermark
- Quality gates enhanced
- 13 unit tests

**v0.4.0** (Planned)
- Historical data tracking (SCD Type 2)
- Effective dating and record versioning

**v0.5.0 → v1.0.0** (Future)
- Advanced analytics modeling
- Data testing framework
- dbt integration
- Cloud migration (AWS)
- BI layer (Tableau/Looker)

**v2.0.0** (Expansion)
- Multi-cloud support
- Apache Spark for big data
- Real-time streaming
- Advanced governance

## Troubleshooting

### Error: "Column 'id' is required but not found in CSV"

**Cause:** CSV structure changed or watermark enabled without proper setup.

**Solution:**

```bash
# Ensure CSV has rows (not empty)
wc -l data/supply_chain_data.csv

# Check watermark table exists
psql -U inventory_user -d inventory_db -c "SELECT * FROM control.watermarks;"

# Re-run initialization
psql -U inventory_user -d inventory_db < sql/006_watermark_tracking.sql
```

### Error: "WATERMARK_ENABLED=true requires CONTROL_SCHEMA to be defined"

**Cause:** Missing environment variable.

**Solution:**

```bash
export CONTROL_SCHEMA=control
python run_pipeline.py
```

### Error: "Database connection refused"

**Cause:** PostgreSQL not running.

**Solution:**

```bash
# Start via Docker
docker-compose up -d postgres

# Or verify local instance
psql -U postgres -c "\l"  # List databases
```

## Support

- **Documentation:** See `docs/` directory
- **Tests:** Run `pytest tests/ -v` for full diagnostics
- **Logs:** Check `logs/` directory for detailed execution history
- **GitHub Issues:** Create an issue

## License

MIT License (see LICENSE file)

---
*Built with ❤️ for data engineers learning production-grade patterns.*
v0.3.0 is stable and tested. Ready for v0.4.0 planning.