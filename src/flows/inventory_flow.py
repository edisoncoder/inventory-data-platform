"""
Prefect flow for inventory ingestion: read CSV -> validate DQ -> write Postgres.
Structured logs, tasks for retry/parallelism later.
Full truncate+load for v0.2 baseline; evolvable to incremental.
"""
import csv
from typing import List, Dict, Any
from prefect import flow, task, get_run_logger
from ..db import get_engine, create_inventory_table
from ..config import get_config

logger = get_run_logger()

@task(retries=2)
def read_inventory_source(csv_path: str) -> List[Dict[str, Any]]:
    logger.info(f"Reading from {csv_path}")
    items = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            items = list(reader)
        logger.info(f"Read {len(items)} raw items")
        return items
    except FileNotFoundError:
        logger.error(f"File not found: {csv_path}")
        return []

@task
def validate_inventory(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    logger.info(f"Validating {len(items)} items")
    valid = []
    invalid_count = 0
    for item in items:
        sku = item.get("sku", "").strip()
        name = item.get("name", "").strip()
        try:
            qty = int(item.get("quantity", 0))
        except ValueError:
            qty = 0
        if sku and name and qty > 0:
            item["quantity"] = qty
            valid.append(item)
        else:
            invalid_count += 1
            logger.warning(f"Invalid item skipped: {item}")
    logger.info(f"{len(valid)} valid, {invalid_count} invalid")
    return valid

@task
def write_inventory_to_db(items: List[Dict[str, Any]]):
    logger.info(f"Writing {len(items)} items to DB")
    engine = get_engine()
    create_inventory_table()
    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE inventory_items RESTART IDENTITY;"))
        for item in items:
            conn.execute(
                sa.text("INSERT INTO inventory_items (sku, name, quantity) VALUES (:sku, :name, :quantity)"),
                {"sku": item["sku"], "name": item["name"], "quantity": item["quantity"]}
            )
    logger.info("Inventory loaded successfully")

@flow(name="inventory-ingestion-flow")
def inventory_flow():
    config = get_config()
    raw_items = read_inventory_source(config["csv_path"])
    if not raw_items:
        logger.error("No data found")
        return
    valid_items = validate_inventory(raw_items)
    write_inventory_to_db(valid_items)
