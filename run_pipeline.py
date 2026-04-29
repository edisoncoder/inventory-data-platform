import time
from pathlib import Path
import subprocess
import sys
from datetime import datetime
import os

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)

def insert_pipeline_log(start_time, end_time, status, rows_processed=None, error_message=None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO analytics.pipeline_runs (
                pipeline_name,
                start_time,
                end_time,
                status,
                rows_processed,
                error_message
            )
            VALUES (
                :pipeline_name,
                :start_time,
                :end_time,
                :status,
                :rows_processed,
                :error_message
            )
        """), {
            "pipeline_name": "supply_chain_pipeline",
            "start_time": start_time,
            "end_time": end_time,
            "status": status,
            "rows_processed": rows_processed,
            "error_message": error_message
        })

def get_row_count():
    with engine.begin() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM raw.supply_chain_data
        """))
        return result.scalar()

def run_step(run_id, step_name, command):
    print(f"\n=== Executando: {step_name} ===")
    step_start = time.time()

    result = subprocess.run(command)

    step_duration = time.time() - step_start

    if result.returncode != 0:
        write_log_line(
            run_id=run_id,
            step_name=step_name,
            status="FAILED",
            duration=step_duration,
            error_message=f"Falha na etapa: {step_name}"
        )
        raise RuntimeError(f"Falha na etapa: {step_name}")

    write_log_line(
        run_id=run_id,
        step_name=step_name,
        status="SUCCESS",
        duration=step_duration
    )

    print(f"=== Etapa concluída: {step_name} ===")

def write_log_line(run_id, step_name, status, rows=None, duration=None, error_message=None):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows_value = str(rows) if rows is not None else ""
    duration_value = f"{duration:.2f}s" if duration is not None else ""
    error_value = str(error_message).replace("\n", " ").replace("\r", " ") if error_message else ""

    line = f"{timestamp}|{run_id}|{step_name}|{status}|{rows_value}|{duration_value}|{error_value}\n"

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line)

def send_alert(run_id, message):
    print("DEBUG: entrou em send_alert()")
    alert_dir = Path("alerts")
    alert_dir.mkdir(exist_ok=True)

    alert_file = alert_dir / f"alerts_{datetime.now().strftime('%Y-%m-%d')}.log"

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    line = f"{timestamp}|{run_id}|ALERT|{message}\n"

    with open(alert_file, "a", encoding="utf-8") as f:
        f.write(line)

    print("\n🚨 ALERTA DE FALHA NO PIPELINE 🚨")
    print(message)

def main():
    start_time = datetime.now()
    start_timer = time.time()
    run_id = f"run_{start_time.strftime('%Y%m%d_%H%M%S')}"

    try:
        run_step(run_id, "Ingestion", [sys.executable, "ingestion/load_supply_chain_csv.py"])
        run_step(run_id, "Raw Quality Checks", [sys.executable, "quality/data_quality_checks.py"])
        run_step(run_id, "Staging Checks", [sys.executable, "quality/staging_checks.py"])
        run_step(run_id, "Analytics Checks", [sys.executable, "quality/analytics_checks.py"])

        rows = get_row_count()
        end_time = datetime.now()
        duration = time.time() - start_timer

        insert_pipeline_log(
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            rows_processed=rows
        )

        write_log_line(
            run_id=run_id,
            step_name="PIPELINE",
            status="SUCCESS",
            rows=rows,
            duration=duration
        )

        print("\nPipeline executado com sucesso.")

    except Exception as exc:
        end_time = datetime.now()
        duration = time.time() - start_timer

        insert_pipeline_log(
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            error_message=str(exc)
        )

        write_log_line(
            run_id=run_id,
            step_name="PIPELINE",
            status="FAILED",
            duration=duration,
            error_message=str(exc)
        )

        send_alert(run_id, str(exc))

        print(f"\n[PIPELINE ERROR] {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()