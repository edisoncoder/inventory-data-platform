import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

from prefect import flow, task
from prefect import get_run_logger

PROJECT_DIR = Path(__file__).resolve().parent


@task(name="Ingestion", retries=2, retry_delay_seconds=10, timeout_seconds=60)
def run_ingestion():
    logger = get_run_logger()

    logger.info("Iniciando ingestion...")

    result = subprocess.run(
        [sys.executable, "ingestion/load_supply_chain_csv.py"],
        cwd=PROJECT_DIR,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Falha na ingestion")
        raise RuntimeError("Falha na etapa: Ingestion")

    logger.info("Ingestion concluída com sucesso")    


@task(name="Raw Quality Checks", retries=2, retry_delay_seconds=10, timeout_seconds=60)
def run_raw_checks():
    logger = get_run_logger()

    logger.info("Iniciando quality...")

    result = subprocess.run(
        [sys.executable, "quality/data_quality_checks.py"],
        cwd=PROJECT_DIR,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Falha na quality")
        raise RuntimeError("Falha na etapa: Raw Quality Checks")

    logger.info("Quality concluída com sucesso")    


@task(name="Staging Checks", retries=2, retry_delay_seconds=10, timeout_seconds=60)
def run_staging_checks():
    logger = get_run_logger()

    logger.info("Iniciando staging...")

    result = subprocess.run(
        [sys.executable, "quality/staging_checks.py"],
        cwd=PROJECT_DIR,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Falha na staging")
        raise RuntimeError("Falha na etapa: Staging Checks")

    logger.info("Staging concluída com sucesso")    


@task(name="Analytics Checks", retries=2, retry_delay_seconds=10, timeout_seconds=60)
def run_analytics_checks():
    logger = get_run_logger()

    logger.info("Iniciando analytics...")

    result = subprocess.run(
        [sys.executable, "quality/analytics_checks.py"],
        cwd=PROJECT_DIR,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        logger.error("Falha na analytics")
        raise RuntimeError("Falha na etapa: Analytics Checks")

    logger.info("Analytics concluída com sucesso")    


@flow(name="supply-chain-pipeline")
def supply_chain_pipeline():
    ingestion = run_ingestion()
    raw = run_raw_checks()
    staging = run_staging_checks()
    analytics = run_analytics_checks()


if __name__ == "__main__":
    supply_chain_pipeline()