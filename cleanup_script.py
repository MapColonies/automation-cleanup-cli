import logging
import os
import datetime

from cleanup_cli.app import executers
from cleanup_cli.wrappers import env, connection

"""
Script for deleting all ingestion tests data such as tile,db records, and mapproxy after running the tests set - 
used to clean the resources and storage from unnecessary data 
"""
now = datetime.datetime.now()
current_date = now.strftime("%Y-%m-%d")

logger = logging.getLogger("cleanup_logger")

logger.setLevel(logging.INFO)

log_file = f"{os.environ.get('LOGS_FILE_PATH')}/{current_date}"
file_handler = logging.FileHandler(log_file)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)

conf_dir = os.environ.get('CONF_FILE')
storage_handler = None
if conf_dir:
    logger.info(f'Configuration file provided in path -{conf_dir}')
    resp = executers.load_json(conf_dir)
    if resp['state']:
        conf = resp['content']
        pg_handler = env.set_pg_wrapper(conf['pg_connection'])
        logger.info("Pg connection created")
        if conf['tiles_provider'].lower() == 's3':
            logger.info(f"Tiles provider - {conf['tiles_provider']} ")
            storage_handler = connection.StorageManager(env.set_s3_wrapper(conf['s3_connection']))
        elif conf['tiles_provider'].lower() == 'fs':
            logger.info(f"Tiles provider - {conf['tiles_provider']} ")
            storage_handler = connection.StorageManager(env.set_fs_wrapper(conf['fs_connection']))
        logger.info("Collecting tests data to delete")
        data_to_clean = pg_handler.get_daily_cleanup_data()
        if not data_to_clean:
            logger.info("No data was found to clean")
        else:
            logger.info(f"Data to clean : {data_to_clean}")
            try:
                logger.info("Start running cleanup")
                resp = executers.run_cleanup(data_file=data_to_clean, pg_handler=pg_handler,
                                             storage_handler=storage_handler)
                logger.info('Cleanup script execution completed')
            except Exception as e:
                resp = str(e)
                logger.error(resp)
            # print(json.dumps(resp))
    else:
        logger.error('Configuration file content is invalid')
else:
    logger.error('No configuration file was provided')
    raise FileNotFoundError
