from dotenv import load_dotenv
import os

def get_config():
    """
    Retorna um dicionário com a configuração essencial da plataforma.
    Prioriza DATABASE_URL, fallback para construção via DB_* ou POSTGRES_*.
    Valida presença de URL ou credenciais básicas.
    """
    load_dotenv()

    # Obtém DATABASE_URL diretamente
    database_url = os.getenv('DATABASE_URL')

    # Extrai credenciais individuais para fallback e referência
    db_host = os.getenv('DB_HOST') or os.getenv('POSTGRES_HOST')
    db_port = os.getenv('DB_PORT') or os.getenv('POSTGRES_PORT', '5432')
    db_user = os.getenv('DB_USER') or os.getenv('POSTGRES_USER')
    db_password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('DB_NAME') or os.getenv('POSTGRES_DB')

    # Se não há URL, tenta construir com credenciais
    if not database_url:
        if not all([db_host, db_user, db_password, db_name]):
            raise ValueError(
                'DATABASE_URL ou credenciais do banco estão ausentes no .env. '
                'Forneça DATABASE_URL ou: '
                '(DB_HOST/POSTGRES_HOST, DB_PORT/POSTGRES_PORT=5432, '
                'DB_USER/POSTGRES_USER, DB_PASSWORD/POSTGRES_PASSWORD, '
                'DB_NAME/POSTGRES_DB).'
            )
        database_url = (
            f"postgresql://{db_user}:{db_password}@"
            f"{db_host}:{db_port}/{db_name}"
        )

    config = {
        'database_url': database_url,
        'csv_path': os.getenv('CSV_PATH'),
        'raw_schema': os.getenv('RAW_SCHEMA', 'raw'),#raw schema
        'analytics_schema': os.getenv('ANALYTICS_SCHEMA', 'analytics'),#analytics schema
        'raw_table': os.getenv('RAW_TABLE', 'supply_chain_data'),#raw schema
        'analytics_table': os.getenv('ANALYTICS_TABLE', 'supplier_summary'),#analytics schema
        'schedule_cron': os.getenv('SCHEDULE_CRON', '0 2 * * *'),
        'adminer_port': int(os.getenv('ADMINER_PORT', '8080')),
        'db_host': db_host,
        'db_port': db_port,
        'db_name': db_name,
        'db_user': db_user,
    }

    return config

