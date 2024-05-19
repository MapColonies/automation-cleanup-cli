"""
This module provide the cli execution business logic and API
"""
import os
import json
from time import sleep

import requests
from mc_automation_tools.base_requests import send_delete_request

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


# def delete_layer_mapproxy_config( layer_names, mapproxy_route):
#     """
#     This method will execute clean on mapproxy config DB and remove related layer's configs
#     :param layer_names: list -> layer to delete
#     :return: dict => {state: bool, msg: dict}
#     """
#     result = []
#     for layer_name in layer_names:
#         layer_name = "-".join([layer_name, "OrthophotoBest"])
#         resp = send_delete_request(url=mapproxy_route, params=layer_name)
#
#         # pg_conn = self._get_connection_to_scheme(self.__mapproxy_scheme)
#         # resp = pg_conn.delete_by_json_key(table_name=self.__mapproxy_config_table,
#         #                                   pk="data",
#         #                                   canonic_keys=["caches"],
#         #                                   value=layer_name)
#         result.append({"layer": layer_name, "result": resp})
#     return result


def run_cleanup(data_file, mapproxy_route, pg_handler=None, storage_handler=None, deletion_list=None):
    """
    This method will execute full cleanup according configuration
    :param data_file: layer to delete
    :param pg_handler: pg connection object
    :param storage_handler: storage connection object
    :param deletion_list: dict -> description of what records to cleanup -> default all (None)
    :return: dict -> results
    """
    results = {}
    if data_file:
        for layer in data_file:
            layer_id = layer['product_id']
            layer_version = layer['product_version']
            layer_type = layer.get('product_type')
            identifier = layer.get("identifier")

            if not deletion_list:
                tiles_storage_params = pg_handler.get_tiles_path_convention(product_id=layer_id)
                tiles_storage_params = tiles_storage_params[0]
                identifier = tiles_storage_params['identifier']
                display_path = tiles_storage_params['display_path']
                tiles_path_convention = f"{identifier}/{display_path}"

                job_task_pg = pg_handler.delete_job_task_by_layer(product_id=layer_id, product_version=layer_version,
                                                                  product_type=layer_type)

                # tasks_delete_api = requests.delete(url=)
                # layer_spec_pg = pg_handler.delete_tile_counter_by_layer(product_id=layer_id, product_version=layer_version)
                pycsw_catalog_pg = pg_handler.delete_record_by_layer(product_id=layer_id, product_version=layer_version,
                                                                     product_type=layer_type)
                # mapproxy_pg = pg_handler.remove_config_mapproxy(product_id=layer_id, product_version=layer_version,
                #                                                 mapproxy_route=mapproxy_route)
                # agent_pg = pg_handler.remove_agent_db_record(product_id=layer_id, product_version=layer_version)
                mapproxy_pg = pg_handler._delete_mapproxy_config(layer_name=layer_id, mapproxy_route=mapproxy_route)
                storage = storage_handler.remove_tiles(layer_name=tiles_path_convention)

                results[layer_id] = {'jobs': job_task_pg,
                                     # 'layer_spec': layer_spec_pg,
                                     'catalog_pycsw': pycsw_catalog_pg,
                                     'mapproxy': mapproxy_pg,
                                     # 'agent': agent_pg,
                                     'storage': storage}
        sleep(5)
        return results
    else:
        return "Data to clean not found- layer list is None"
