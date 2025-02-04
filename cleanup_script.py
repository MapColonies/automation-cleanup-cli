import logging
import datetime
import os

from cleanup_cli.app import executers
from cleanup_cli.wrappers import env, connection
from mc_automation_tools.parse import stringy


"""
This script is used for cleaning our resources from unnecessary data from our tests ( nightly, manual tests.. ) 
The data which will be removed : tiles from S3, DB records such as mapproxy, jobs & tasks and catalog records.
"""

def init_logger():
    now = datetime.datetime.now()
    current_date = now.strftime("%Y-%m-%d")
    log_output_path = f"{os.environ.get('LOGS_FILE_PATH')}/{current_date}"

    # init logger
    logger = logging.getLogger("cleanup_logger")
    file_handler = logging.FileHandler(log_output_path)
    logger.addHandler(file_handler)
    logger.setLevel(logging.INFO)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    return logger

def log_data_to_clean(logger,data_to_clean):
    logger.info(f"Found {len(data_to_clean)} layers to clean")
    logger.info("\n" + stringy.pad_with_stars(
        "The ID's of the layers found to be clean", length=138))
    layers_ids = []
    for layer in data_to_clean:
        layers_ids.append(layer["identifier"])
    logger.info(layers_ids)


if __name__ == '__main__':
    logger = init_logger()
    conf_dir = os.environ.get('CONF_FILE')
    if conf_dir:
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

            # Set routes
            mapproxy_config_route = conf["mapproxy_config_route"]
            job_manager_route = conf["job_manager_route"]
            raster_catalog_route = conf["raster_catalog_route"]
            token = conf["X-API-KEY"]

            logger.info("Collecting tests data to delete")
            data_to_clean = pg_handler.get_daily_cleanup_data()
            if not data_to_clean:
                logger.info("No data was found to clean")
            else:
                log_data_to_clean(logger,data_to_clean)
                try:
                    logger.info("\n" + stringy.pad_with_stars(
                        "Start running cleanup" , length=140))
                    resp = executers.run_cleanup(deletion_list=data_to_clean,
                                                 pg_handler= pg_handler,
                                                 storage_handler=storage_handler, mapproxy_route=mapproxy_config_route,
                                                 job_manager_route=job_manager_route,
                                                 raster_catalog_route=raster_catalog_route, token=token, logger=logger)
                    logger.info("\n" + stringy.pad_with_stars(
                        "Cleanup script execution completed", length=140))
                except Exception as e:
                    resp = str(e)
                    logger.error(resp)
        else:
            logger.error('Configuration file content is invalid')
    else:
        logger.error('No configuration file was provided')
        raise FileNotFoundError
