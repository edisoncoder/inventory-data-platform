# Pipeline Orchestration

## Transition from v0.1.0 to v0.2.0

In v0.1.0, execution was centered on cron jobs, functional for simple cases but with limitations in monitoring, retries, and visibility.

v0.2.0 formalizes **Prefect** as the primary orchestrator, positioning cron as legacy support.

**Reasons for the change:**
- Observability: dashboards, logs, and metrics.
- Automatic retries and error handling.
- Parameterization and future scalability.
- Industry standard for data workflows.

## Architecture with Prefect

- **Main flow**: `prefect_pipeline.py` - defines tasks for ingestion, normalization, and checks on `supply_chain_data.csv`.
- **Manual execution**: `python run_pipeline.py` - runs the flow locally without a server.
- **Local scheduling**: `python scheduler.py` - starts a local Prefect server with schedule.

Configuration in `src/config.py`.

## How to Run Locally

1. Install dependencies: `pip install -r requirements.txt`
2. **Manual (legacy)**: `bash run_pipeline.sh` or `python run_pipeline.py`
3. **Scheduled (official)**: `python scheduler.py` (Ctrl+C to stop)

## Next Steps (v0.3.0)

- Prefect Cloud.
- Incremental executions.
- Database integration.
