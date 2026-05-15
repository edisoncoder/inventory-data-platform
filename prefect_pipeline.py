import pandas as pd
from prefect import flow, task
from sqlalchemy import create_engine, text
from src.config import get_config
from ingestion.load_supply_chain_csv import load_supply_chain_csv, TEXT_COLUMNS, NUMERIC_COLUMNS, IDENTIFIER_COLUMNS
from quality.data_quality_checks import run_quality_checks
from prefect.cache_policies import NO_CACHE

def map_to_pg_dtype(dtype_name):
    """Mapeia dtype do pandas para PostgreSQL."""
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
    """Cria schema e tabela com DDL explícito (idempotente)."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {schema};'))
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {schema}.{table} CASCADE;'))
    col_defs = [f'"{col}" {map_to_pg_dtype(dtype_name)}' for col, dtype_name in dtypes.items()]
    create_sql = f'CREATE TABLE {schema}.{table} ({", ".join(col_defs)});'
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    print(f'Tabela {schema}.{table} criada com sucesso.')

@task(cache_policy=NO_CACHE)
def load_csv_task(csv_path: str):
    return load_supply_chain_csv(csv_path)

@task(cache_policy=NO_CACHE)
def quality_check_task(df):
    return run_quality_checks(df)

@task(cache_policy=NO_CACHE)
def get_engine_task():
    config = get_config()
    database_url = config["database_url"]
    return create_engine(database_url, pool_pre_ping=True, echo=False)

@task(cache_policy=NO_CACHE)
def load_raw_task(df, engine):
    config = get_config()
    schema = config["raw_schema"]
    table = config["raw_table"]
    dtypes = {col: df[col].dtype.name for col in df.columns}
    setup_schema_and_table(engine, schema, table, dtypes)
    df.to_sql(name=table, schema=schema, con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
    print(f'Dados raw carregados: {len(df)} linhas em {schema}.{table}.')

@task(cache_policy=NO_CACHE)
def load_analytics_task(df, engine):
    # Transformações simples para analytics
    config = get_config()
    schema = config["analytics_schema"]
    table = config["analytics_table"]
    df_clean = df.copy()
    for col in NUMERIC_COLUMNS:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
    for col in TEXT_COLUMNS:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].fillna('DESCONHECIDO')
    df_clean = df_clean.drop_duplicates(subset=IDENTIFIER_COLUMNS, keep='last')
    dtypes = {col: df_clean[col].dtype.name for col in df_clean.columns}
    setup_schema_and_table(engine, schema, table, dtypes)
    df_clean.to_sql(name=table, schema=schema, con=engine, if_exists='append', index=False, chunksize=1000, method='multi')
    print(f'Dados analytics carregados: {len(df_clean)} linhas em {schema}.{table}.')

@flow(name='supply_chain_pipeline_v0_2_0')
def supply_chain_pipeline(csv_path:str,db_url:str):
    """Pipeline Prefect 2.x completo para supply chain (full refresh idempotente)."""
    print('=== Iniciando Pipeline Supply Chain v0.2.0 ===')
    config = get_config()
    csv_path = config["csv_path"]
    df = load_csv_task(csv_path)
    quality = quality_check_task(df)
    print('Relatório de qualidade:', quality)
    engine = get_engine_task()
    load_raw_task(df, engine)
    load_analytics_task(df, engine)
    engine.dispose()
    print('=== Pipeline concluído com sucesso! ===')
