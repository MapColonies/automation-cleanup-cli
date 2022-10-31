"""
This module provide the cli execution business logic and API
"""
import os
import json
from time import sleep

from cleanup_cli.wrappers import env
from cleanup_cli.wrappers.connection import *


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


def run_cleanup(data_file, pg_handler=None, storage_handler=None, deletion_list=None):
    """
    This method will execute full cleanup according configuration
    :param data_file: layer to delete
    :param pg_handler: pg connection object
    :param storage_handler: storage connection object
    :param deletion_list: dict -> description of what records to cleanup -> default all (None)
    :return: dict -> results
    """
    results = {}
    for layer in data_file:
        layer_id = layer['product_id']
        layer_version = layer['product_version']
        layer_type = layer.get('product_type')

        if not deletion_list:
            tiles_storage_params = pg_handler.get_tiles_path_convention(product_id=layer_id)
            identifier = tiles_storage_params['identifier']
            display_path = tiles_storage_params['display_path']
            tiles_path_convention = f"{identifier}/{display_path}"
            job_task_pg = pg_handler.delete_job_task_by_layer(product_id=layer_id, product_version=layer_version,
                                                              product_type=layer_type)
            layer_spec_pg = pg_handler.delete_tile_counter_by_layer(product_id=layer_id, product_version=layer_version)
            pycsw_catalog_pg = pg_handler.delete_record_by_layer(product_id=layer_id, product_version=layer_version,
                                                                 product_type=layer_type)
            mapproxy_pg = pg_handler.remove_config_mapproxy(product_id=layer_id, product_version=layer_version)
            # mapproxy_pg = {}
            agent_pg = pg_handler.remove_agent_db_record(product_id=layer_id, product_version=layer_version)
            # todo: add the new convention
            storage = storage_handler.remove_tiles(layer_name=tiles_path_convention)

            results[layer_id] = {'jobs': job_task_pg,
                                 'layer_spec': layer_spec_pg,
                                 'catalog_pycsw': pycsw_catalog_pg,
                                 'mapproxy': mapproxy_pg,
                                 'agent': agent_pg,
                                 'storage': storage}
    sleep(5)
    return results
