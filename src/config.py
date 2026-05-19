from dotenv import load_dotenv
import os

def get_config():
    """
    Function to obtain centralized configuration as a dictionary.
    Maintains compatibility with v0.2.0.
    Supports environment variables and performs validations.
    Returns a dictionary with the essential platform configuration.
    Prioritizes DATABASE_URL, fallback for building via DB_* or POSTGRES_*.
    Validates the presence of a URL or basic credentials.
    """
    load_dotenv()

    # Get DATABASE_URL directly
    database_url = os.getenv('DATABASE_URL')

    # Reading environment variables with defaults
    watermark_table = os.getenv('WATERMARK_TABLE', 'watermark')
    control_schema = os.getenv('CONTROL_SCHEMA', 'control')
    incremental_mode = os.getenv('INCREMENTAL_MODE', 'false').lower() in ('true', '1', 'yes', 'on')
    watermark_enabled = os.getenv('WATERMARK_ENABLED', 'false').lower() in ('true', '1', 'yes', 'on')
 
    # Extracts individual credentials for fallback and referral purposes.
    db_host = os.getenv('DB_HOST') or os.getenv('POSTGRES_HOST')
    db_port = os.getenv('DB_PORT') or os.getenv('POSTGRES_PORT', '5432')
    db_user = os.getenv('DB_USER') or os.getenv('POSTGRES_USER')
    db_password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD')
    db_name = os.getenv('DB_NAME') or os.getenv('POSTGRES_DB')

    # If there is no URL, try building it with credentials.
    if not database_url:
        if not all([db_host, db_user, db_password, db_name]):
            raise ValueError(
                'The DATABASE_URL or database credentials are missing from the .env file. '
                'Provide DATABASE_URL or: '
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
        'watermark_table': watermark_table,
        'control_schema': control_schema,
        'incremental_mode': incremental_mode,
        'watermark_enabled': watermark_enabled,
    }

    # Validation: watermark_enabled requires control_schema
    if watermark_enabled and not control_schema:
        raise ValueError(
            'WATERMARK_ENABLED=true requires CONTROL_SCHEMA defined '
            '(ex: export CONTROL_SCHEMA=\"control\").'
        )

    return config

