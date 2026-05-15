# run_pipeline.py - v0.2.0
#
# Manually execute the Prefect supply_chain_pipeline flow
# without scheduling or a server, ideal for local debugging.

import logging
import sys
from prefect_pipeline import supply_chain_pipeline
from src.config import get_config

# Detailed logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    try:
        logger.info('=== Starting manual execution of pipeline v0.2.0 ===')
        
        logger.info('Reading environment variables via src.config.get_config()...')
        config = get_config()
        csv_path = config['csv_path']
        db_url = config['database_url']
        raw_table = config['raw_table']
        
        logger.info('Configuration loaded successfully..')
        logger.debug(f'Config keys: {list(config.keys())}')  # Debug: não loga valores sensíveis
        
        logger.info('Executing the supply_chain_pipeline flow synchronously....')
        result = supply_chain_pipeline(csv_path,db_url)
        
        logger.info('=== Pipeline completed successfully.! ===')
        logger.info(f'Flow result: {result}')
        
    except Exception as e:
        logger.error('=== ERROR executing the pipeline ===')
        logger.error(f'Error details: {str(e)}')
        logger.error('Check the debug logs. Make sure that:')
        logger.error('- Prefect is installed.')
        logger.error('- prefect_pipeline.py is accessible')
        logger.error('- Environment variables are defined.')
        sys.exit(1)

    logger.info('Script completed.')