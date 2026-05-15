# ETL Pipeline v0.2.0

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/Prefect-2.x-orange?logo=prefect&logoColor=white)](https://www.prefect.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-lightblue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)

## Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline with Prefect orchestration, ensuring data reliability through ACID compliance, Dependency Injection patterns, and quality gates.

### Problem

Organizations struggle with:
- Unreliable pipelines prone to failures and data inconsistencies
- Lack of observability and monitoring
- Manual reprocessing and lack of idempotency
- No quality assurance for data integrity

### Solution

A production-grade ETL platform built on modern data engineering principles: **Prefect** for orchestration, **PostgreSQL** for durable storage with ACID guarantees, and **Python** for flexible transformations. Version 0.2.0 features full orchestration, quality gates, and structured logging.

## Architecture
CSV Source --> Prefect Flow --> Extract Task --> Transform Task --> Load Task --> PostgreSQL
|
Quality Gates (Pydantic)
|
ACID Transactions (SQLAlchemy 2.x)
|
Logging + Observability

**Key Components:**
- **Prefect Flows**: Orchestrated ETL workloads with retries and state management
- **Tasks**: Modular, reusable units (extract, validate, transform, load)
- **Quality Gates**: Data validation before and after each stage
- **ACID Compliance**: All database operations wrapped in transactions
- **Dependency Injection**: Configuration-driven, testable task parameters

## Tech Stack

- **Orchestration**: Prefect 2.x
- **Language**: Python 3.9+
- **Database**: PostgreSQL 15+
- **Data Processing**: Pandas, SQLAlchemy 2.x
- **Validation**: Pydantic
- **Configuration**: python-dotenv

## Requirements

**System:**
- Python 3.9 or higher
- PostgreSQL 15+ (running instance)
- Docker (optional)

**Install dependencies:**

    pip install -r requirements.txt

**Key packages (requirements.txt):**

    prefect>=2.10.0
    sqlalchemy>=2.0.0
    psycopg2-binary>=2.9.0
    pandas>=2.0.0
    pydantic>=2.0.0
    python-dotenv>=1.0.0

**Environment variables (.env):**

    DATABASE_URL=postgresql://user:password@localhost:5432/supply_chain_db
    CSV_PATH=data/supply_chain.csv
    RAW_SCHEMA=raw
    RAW_TABLE=supply_chain_data
    ANALYTICS_SCHEMA=analytics
    ANALYTICS_TABLE=supply_chain_summary
    SCHEDULE_CRON=0 2 * * *

## Quick Start

1. Clone the repository:

       git clone https://github.com/edisoncoder/inventory-data-platform.git
       cd inventory-data-platform

2. Create virtual environment:

       python -m venv venv
       source venv/bin/activate  # Linux/Mac
       **# or: venv\Scripts\activate  # Windows**

3. Install dependencies:

       pip install -r requirements.txt

4. Configure environment variables:

       cp .env.example .env
       **# Edit .env with your PostgreSQL credentials**

5. Start environment

       docker-compose up -d

6. Start Prefect server:

       prefect server start

7. Run the pipeline:

       python run_pipeline.py

## Execution Methods

### Method 1: Manual Execution (Development)

    python run_pipeline.py

Executes the flow synchronously with structured logging to console and file.

### Method 2: Scheduled via Cron (Operational)

    crontab -e

Add entry:

    0 2 * * * cd /path/to/project && python run_pipeline.py >> /var/log/pipeline.log 2>&1

### Method 3: Prefect Scheduler (Future - v0.3.0)

When integrated with Prefect Cloud or advanced deployments.

## What's Implemented (v0.2.0)

✅ **Prefect 2.x Orchestration**
- Flow-based task orchestration
- Retry logic with exponential backoff
- Logging via Prefect UI and structured logs

✅ **ACID Compliance**
- Transactional database operations (engine.begin() with auto-commit/rollback)
- Atomicity: All-or-nothing inserts
- Consistency: Schema validation via Pydantic
- Isolation: Connection pooling with row-level locking
- Durability: PostgreSQL WAL (Write-Ahead Logging)

✅ **Dependency Injection**
- Configuration loaded once per flow (get_config())
- Values distributed to tasks via parameters
- No task-level config access (loose coupling)

✅ **Quality Gates**
- Pre-load validation: schema checks, duplicate detection, null handling
- Post-load assertions: row count verification, aggregate consistency
- Fail-fast on validation errors

✅ **Logging & Observability**
- Structured logging to console and files
- Prefect task-level logging
- Daily log rotation

✅ **Idempotent Processing**
- Safe re-runs without side effects
- Full refresh strategy (drop and recreate tables)

## File Structure.
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .env.example                 # Environment template
├── run_pipeline.py              # Manual execution entry point
├── scheduler.py                 # Scheduler reference guide
├── prefect_pipeline.py          # Prefect flow definition
├── src/
│   ├── init.py
│   └── config.py                # Configuration loader (get_config)
├── data/
│   └── supply_chain.csv         # Input data
├── logs/                        # Pipeline execution logs
└── docs/
   └── ARCHITECTURE.md          # Detailed architecture
   ├── CHANGELOG.md             # Version history
   └── VERSIONAMENTO.md         # Git tagging guide

## Roadmap

### v0.2.0 (Current)
- ✅ Prefect 2.x orchestration
- ✅ ACID transactions
- ✅ Dependency Injection patterns
- ✅ Quality gates (structural)

### v0.3.0 (Planned)
- Incremental load with watermark tables
- Historical data tracking (SCD Type 2)
- dbt integration for transformations
- Slack/Email alerting

### v1.0.0 (Target)
- Prefect Cloud deployment
- Kubernetes workers
- Advanced monitoring (Prometheus/Grafana)
- Multi-source support (API, S3, Kafka)
- Full test coverage (unit, integration, E2E)

### Future (v2.0.0+)
- ML model integration
- Real-time streaming (Kafka)
- AWS cloud migration
- dbt + BI tool integration

## Current Status

**Version**: 0.2.0 (Stable - Ready for staging)
**Last Updated**: May 2026
**Test Coverage**: ~80%
**Production Readiness**: Staging-ready (local deployment validated)

## Contributing

1. Create feature branch from `main`
2. Follow Clean Code principles
3. Add tests for new features
4. Update CHANGELOG.md
5. Create pull request

## Support

Report issues: [GitHub Issues](#)
Documentation: See `docs/` directory

---

*Built with ❤️ for data engineers learning production-grade patterns.*