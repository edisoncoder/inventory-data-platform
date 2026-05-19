# src/config.py
# Supports v0.2.0 (legacy) and v0.3.0 (incremental) modes

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_config():
    """
    Centralized configuration loader using environment variables from .env
    
    Returns:
        dict: Configuration dictionary with all required keys for pipeline execution
    """
    
    # Database connection parameters
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'supply_chain_db')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    
    # Build PostgreSQL connection URL
    database_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    
    # v0.2.0 Configuration (preserved for backward compatibility)
    config = {
        # Connection
        'database_url': database_url,
        'db_host': db_host,
        'db_port': int(db_port),
        'db_name': db_name,
        'db_user': db_user,
        
        # Data source
        'csv_path': os.getenv('CSV_PATH', 'data/supply_chain_data.csv'),
        
        # Schema configuration (raw layer)
        'raw_schema': os.getenv('RAW_SCHEMA', 'raw'),
        'raw_table': os.getenv('RAW_TABLE', 'supply_chain_data'),
        
        # Schema configuration (analytics layer)
        'analytics_schema': os.getenv('ANALYTICS_SCHEMA', 'analytics'),
        'analytics_table': os.getenv('ANALYTICS_TABLE', 'supplier_summary'),
        
        # Scheduling (cron)
        'schedule_cron': os.getenv('SCHEDULE_CRON', '0 2 * * *'),
        
        # Service ports
        'adminer_port': int(os.getenv('ADMINER_PORT', '8080')),
        
        # v0.3.0 Configuration (incremental load with watermark)
        'watermark_enabled': os.getenv('WATERMARK_ENABLED', 'false').lower() in ('true', '1', 'yes', 'on'),
        'control_schema': os.getenv('CONTROL_SCHEMA', 'control'),
        'watermark_table': os.getenv('WATERMARK_TABLE', 'watermarks'),
    }
    
    # Validation: watermark mode requires control schema
    if config['watermark_enabled'] and not config['control_schema']:
        raise ValueError(
            'WATERMARK_ENABLED=true requires CONTROL_SCHEMA to be defined. '
            'Set in .env: CONTROL_SCHEMA=control'
        )
    
    return config


if __name__ == '__main__':
    # Debug: print current configuration
    cfg = get_config()
    for key, value in cfg.items():
        # Hide sensitive data in output
        if key in ('db_password',):
            print(f'{key}: ****')
        else:
            print(f'{key}: {value}')