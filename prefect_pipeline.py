# prefect_pipeline.py
# Prefect 2.x ETL Pipeline - v0.3.0 (Incremental Load with Watermark)
# Maintains full backward compatibility with v0.2.0 (snapshot mode)

import pandas as pd
from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from src.config import get_config
from ingestion.load_supply_chain_csv import load_supply_chain_csv, TEXT_COLUMNS, NUMERIC_COLUMNS, IDENTIFIER_COLUMNS
from quality.data_quality_checks import run_quality_checks
from prefect.cache_policies import NO_CACHE


def map_to_pg_dtype(dtype_name):
    """Maps pandas dtype to PostgreSQL type."""
    mapping = {
        'object': 'TEXT',
        'string': 'TEXT',
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'float64': 'DOUBLE PRECISION',
        'float32': 'REAL',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }
    return mapping.get(dtype_name, 'TEXT')


def setup_schema_and_table(engine, schema, table, dtypes):
    """Creates schema and table with explicit DDL (idempotent)."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
    
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE;'))
    
    col_defs = [f'"{col}" {map_to_pg_dtype(dtype_name)}' for col, dtype_name in dtypes.items()]
    create_sql = f'CREATE TABLE {schema}.{table} ({", ".join(col_defs)});'
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))


# ============================================================================
# v0.3.0 INCREMENTAL LOAD TASKS
# ============================================================================

@task(cache_policy=NO_CACHE)
def get_watermark(table_name: str, database_url: str) -> int:
    """
    Retrieves the last processed ID (watermark) from control.watermarks table.
    Returns 0 if table doesn't exist or no watermark found (first run).
    
    Args:
        table_name: Name of the table to track (e.g., 'supply_chain_data')
        database_url: PostgreSQL connection URL
    
    Returns:
        Last processed ID (watermark value), or 0 if not found
    """
    logger = get_run_logger()
    engine = create_engine(database_url)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT last_processed_id FROM control.watermarks WHERE table_name = :name"),
                {"name": table_name}
            ).scalar()
        watermark = result if result is not None else 0
        logger.info(f"Watermark retrieved for '{table_name}': {watermark}")
        return watermark
    except Exception as e:
        logger.warning(f"Could not retrieve watermark (likely first run): {e}")
        return 0
    finally:
        engine.dispose()


@task(cache_policy=NO_CACHE)
def load_incremental_data(csv_path: str, watermark: int) -> tuple:
    """
    Loads CSV and filters only rows where id > watermark.
    Returns filtered DataFrame and max_id for watermark update.
    
    Args:
        csv_path: Path to source CSV file
        watermark: Last processed ID (checkpoint)
    
    Returns:
        Tuple of (DataFrame with new data, max_id of new data)
    """
    logger = get_run_logger()
    logger.info(f"Loading CSV from {csv_path} with watermark={watermark}")
    
    df = pd.read_csv(csv_path)
    
    # Add row index as 'id' for watermark tracking
    df.insert(0, 'id', range(1, len(df) + 1))
    
    # Filter only new rows
    df_new = df[df['id'] > watermark].copy()
        
    if df_new.empty:
        logger.info("No new data found (all rows already processed)")
        return df_new, watermark
    
    max_id = int(df_new['id'].max())
    logger.info(f"Found {len(df_new)} new rows (IDs: {watermark+1} to {max_id})")
    
    return df_new, max_id


@task(cache_policy=NO_CACHE)
def quality_gate_incremental(df: pd.DataFrame) -> bool:
    """
    Validates incremental data before ingestion.
    Fails fast if data quality issues detected.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        True if valid, raises ValueError if invalid
    """
    logger = get_run_logger()
    
    if df.empty:
        logger.info("DataFrame is empty, quality gate PASSED (no data to validate)")
        return True
    
    # Check for required columns
    if 'id' not in df.columns:
        raise ValueError("Required column 'id' missing from DataFrame")
    
    # Check for nulls in 'id'
    if df['id'].isnull().any():
        raise ValueError("Null values found in 'id' column")
    
    # Check for duplicates in 'id'
    if df['id'].duplicated().any():
        dup_count = df['id'].duplicated().sum()
        raise ValueError(f"Found {dup_count} duplicate IDs")
     
    logger.info(f"Quality gate PASSED for {len(df)} rows")
    return True


@task(cache_policy=NO_CACHE)
def update_watermark(table_name: str, max_id: int, database_url: str) -> None:
    """
    Updates watermark after successful ingestion.
    Uses ACID transaction (auto-rollback on failure).
    
    CRITICAL: Only executes if preceding tasks (ingest) succeeded.
    Prefect enforces task dependencies, so watermark only advances on success.
    
    Args:
        table_name: Name of tracked table
        max_id: New watermark value (max ID just processed)
        database_url: PostgreSQL connection URL
    """
    logger = get_run_logger()
    engine = create_engine(database_url)
    
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE control.watermarks 
                SET last_processed_id = :max_id,
                    last_processed_timestamp = CURRENT_TIMESTAMP
                WHERE table_name = :table_name
            """),
            {"max_id": max_id, "table_name": table_name}
        )
    
    logger.info(f"Watermark updated: '{table_name}' -> {max_id}")
    engine.dispose()


# ============================================================================
# v0.2.0 LEGACY TASKS (PRESERVED FOR SNAPSHOT MODE)
# ============================================================================

@task(cache_policy=NO_CACHE)
def load_csv_task(csv_path: str):
    """Load complete CSV (v0.2.0 snapshot mode)."""
    logger = get_run_logger()
    logger.info(f"Loading complete CSV from {csv_path}")
    return load_supply_chain_csv(csv_path)


@task(cache_policy=NO_CACHE)
def quality_check_task(df):
    """Run quality checks on DataFrame (v0.2.0)."""
    logger = get_run_logger()
    logger.info("Running quality checks")
    return run_quality_checks(df)


@task(cache_policy=NO_CACHE)
def get_engine_task(database_url: str):
    """Create database engine with connection pooling."""
    logger = get_run_logger()
    logger.info("Creating database engine")
    return create_engine(database_url, pool_pre_ping=True, echo=False)


@task(cache_policy=NO_CACHE)
def load_raw_task(df: pd.DataFrame, engine, config: dict):
    """Ingest data into raw layer (both incremental and snapshot)."""
    logger = get_run_logger()
    schema = config["raw_schema"]
    table = config["raw_table"]
    dtypes = {col: df[col].dtype.name for col in df.columns}
    
    setup_schema_and_table(engine, schema, table, dtypes)
    
    df.to_sql(
        name=table, 
        schema=schema, 
        con=engine, 
        if_exists='append', 
        index=False, 
        chunksize=1000, 
        method='multi'
    )
    
    logger.info(f"Raw data loaded: {len(df)} rows into {schema}.{table}")


@task(cache_policy=NO_CACHE)
def load_analytics_task(df: pd.DataFrame, engine, config: dict):
    """Build analytics layer from raw data."""
    logger = get_run_logger()
    schema = config["analytics_schema"]
    table = config["analytics_table"]
    
    # Simple transformations for analytics
    df_clean = df.copy()
    
    for col in NUMERIC_COLUMNS:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    
    for col in TEXT_COLUMNS:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna('UNKNOWN')
    
    df_clean = df_clean.drop_duplicates(subset=IDENTIFIER_COLUMNS, keep='last')
    
    dtypes = {col: df_clean[col].dtype.name for col in df_clean.columns}
    setup_schema_and_table(engine, schema, table, dtypes)
    
    df_clean.to_sql(
        name=table, 
        schema=schema, 
        con=engine, 
        if_exists='append', 
        index=False, 
        chunksize=1000, 
        method='multi'
    )
    
    logger.info(f"Analytics data loaded: {len(df_clean)} rows into {schema}.{table}")


# ============================================================================
# MAIN FLOW - v0.3.0 (INCREMENTAL + LEGACY)
# ============================================================================

@flow(name='supply_chain_pipeline_v0_3_0')
def supply_chain_pipeline(csv_path: str, database_url: str):
    """
    Main ETL pipeline orchestration.
    
    Supports two execution modes:
    - WATERMARK_ENABLED=true  → Incremental load (process only new data)
    - WATERMARK_ENABLED=false → Snapshot mode (v0.2.0 full refresh)
    
    Always executes analytics layer transformation at the end.
    """
    logger = get_run_logger()
    logger.info('=== Starting Supply Chain Pipeline v0.3.0 ===')
    
    # Load configuration
    config = get_config()
    csv_path = config['csv_path']
    database_url = config['database_url']
    watermark_enabled = config.get('watermark_enabled', False)
    
    logger.info(f"Watermark mode: {'ENABLED' if watermark_enabled else 'DISABLED (v0.2.0 snapshot)'}")
    
    # Get database engine
    engine = get_engine_task(database_url)
    
    # BRANCHING: Incremental vs Legacy
    if watermark_enabled:
        # ===== v0.3.0 INCREMENTAL LOAD PATH =====
        logger.info("🚀 Executing INCREMENTAL LOAD mode")
        
        # Step 1: Get last processed ID (checkpoint)
        watermark = get_watermark('supply_chain_data', database_url)
        
        # Step 2: Load only new data
        df_new, max_id = load_incremental_data(csv_path, watermark)
        
        # If no new data, exit early
        if df_new.empty:
            logger.info("No new data to process. Exiting.")
            engine.dispose()
            return "SUCCESS: No new data"
        
        # Step 3: Quality gate (fail-fast)
        quality_gate_incremental(df_new)
        
        # Step 4: Ingest to raw layer (append mode)
        load_raw_task(df_new, engine, config)
        
        # Step 5: Update watermark (critical: only after successful ingest)
        update_watermark('supply_chain_data', max_id, database_url)
        
        logger.info(f"✅ Incremental load complete: {len(df_new)} rows processed")
    else:
        # ===== v0.2.0 SNAPSHOT MODE (LEGACY) =====
        logger.info("📸 Executing LEGACY SNAPSHOT mode (v0.2.0)")
        
        # Step 1: Load complete CSV
        df = load_csv_task(csv_path)
        
        # Step 2: Quality checks
        quality = quality_check_task(df)
        logger.info(f"Quality report: {quality}")
        
        # Step 3: Full refresh (truncate + insert)
        load_raw_task(df, engine, config)
        
        logger.info(f"✅ Snapshot load complete: {len(df)} rows loaded")
    
    # ANALYTICS LAYER (executed in both modes)
    load_analytics_task(df if not watermark_enabled else df_new, engine, config)
    
    # Cleanup
    engine.dispose()
    
    logger.info('=== Pipeline completed successfully! ===')
    return "SUCCESS"


if __name__ == '__main__':
    supply_chain_pipeline()