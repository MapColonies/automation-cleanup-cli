"""
This module provide the cli execution business logic and API
"""
import os
import json
from time import sleep
from typing import List

from cleanup_cli.utils import delete_record_by_id, delete_layer_from_mapproxy, \
    delete_jobs_tasks_by_ids


def load_json(directory):
    """
    Load and validate json file
    :param directory: str -> path to file
    :return: json file
    """
    results = {}
    if not os.path.exists(directory):
        results['state'] = False
        results['content'] = 'File not exists'
    try:
        with open(directory, "r") as fp:
            file = json.load(fp)
            results['state'] = True
            results['content'] = file

    except Exception as e:
        results['state'] = False
        results['content'] = f'Failed load {directory} with error: {str(e)}'

    return results


def run_cleanup(deletion_list: List[dict], pg_handler, storage_handler, mapproxy_route, job_manager_route,
                raster_catalog_route,
                token, logger):
    """
    This method will execute full cleanup according configuration
    :param deletion_list: dict -> description of what records to clean
    :param storage_handler: storage connection object
    :return: dict -> results
    """
    results = {}
    mapproxy_deletion_list = []
    if deletion_list:
        for layer in deletion_list:
            layer_id = layer.get('product_id')
            layer_type = layer.get('product_type')
            identifier = layer.get("identifier")
            display_path = layer.get("display_path")
            mapproxy_deletion_list.append(f"{layer_id}-{layer_type}")
            tiles_path_convention = f"{identifier}/{display_path}"
            pp_tables = [f"{layer_id}_{layer_type}", f"{layer_id}_{layer_type}_parts"]

            storage = storage_handler.remove_tiles(layer_name=tiles_path_convention)
            catalog_record = delete_record_by_id(record_id=identifier, catalog_manager_url=raster_catalog_route)
            job_task_records = delete_jobs_tasks_by_ids(job_manager_url=job_manager_route, product_id=layer_id,
                                                        token=token)
            pp_tables_status = pg_handler.remove_polygon_parts_table(table_names=pp_tables)

            results[layer_id] = {'jobs': job_task_records,
                                 'catalog_pycsw': catalog_record,
                                 'storage': storage,
                                 'polygon_parts_tables': pp_tables_status}
            logger.info(f"The layer: {identifier} has been cleaned with the results: {results[layer_id]} \n")

        mapproxy_config = delete_layer_from_mapproxy(layers_ids=mapproxy_deletion_list, mapproxy_url=mapproxy_route)

        results["mapproxy_deletion"] = mapproxy_config
        logger.info(f"results of mapproxy deletion : {results['mapproxy_deletion']} \n")

        sleep(5)
        return results
    else:
        return "Data to clean not found - layer list is None"
