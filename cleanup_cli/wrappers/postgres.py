"""
This module will wrap all functionality interfacing with data on DB (postgres)
"""
import json
import logging
import copy

import requests
from mc_automation_tools import postgres
from mc_automation_tools.parse import stringy

# _log = logging.getLogger('sync_tester.cleanup_cli.postgres')
_log = logging.getLogger('cleanup_logger')


class PostgresHandler:

    def __init__(self, pg_credential):
        """
        Initial postgres connection object that contain deployment relevant credits to query and use CRUD operation
        :param pg_credential: dict [json] -> sample:
        pg_credential =  {
                  "pg_endpoint_url": string [ip],
                  "pg_port": int [default is 5432],
                  "pg_user": string,
                  "pg_pass": string,
                  "pg_db": {
                    "name": string,
                    "schemes": {
                      "discrete_agent": {
                        "name": string,
                        "tables": {
                          "layer_history": string
                        }
                      },
                      "job_manager": {
                        "name": string,
                        "tables": {
                          "job": string,
                          "task": string
                        }
                      },
                      "layer_spec": {
                        "name": string,
                        "tables": {
                          "tiles_counter": string
                        },
                        "indexes": {
                          "tiles_counter_id_seq": string
                        }
                      },
                      "mapproxy_config": {
                        "name": string,
                        "tables": {
                          "config": string
                        },
                        "indexes": {
                          "config_id_seq": string
                        }
                      },
                      "raster_catalog_manager": {
                        "name": string,
                        "tables": {
                          "records": string
                        }
                      }
                    }
                  }
                }
        """
        self.__end_point_url = pg_credential.get('pg_endpoint_url')
        self.__port = pg_credential.get('pg_port')
        self.__user = pg_credential.get('pg_user')
        self.__password = pg_credential.get('pg_pass')
        self.__db_name = pg_credential.get('pg_db').get('name')

        # self.__discrete_agent_scheme = pg_credential.get('pg_db')['schemes']['discrete_agent']['name']
        # self.__discrete_agent_table = pg_credential.get('pg_db')['schemes']['discrete_agent']['tables']['layer_history']

        self.__job_manager_scheme = pg_credential.get('pg_db')['schemes']['job_manager']['name']
        self.__job_manager_jobs_table = pg_credential.get('pg_db')['schemes']['job_manager']['tables']['job']
        self.__job_manager_tasks_table = pg_credential.get('pg_db')['schemes']['job_manager']['tables']['task']

        # self.__layer_spec_scheme = pg_credential.get('pg_db')['schemes']['layer_spec']['name']
        # self.__tiles_counter_table = pg_credential.get('pg_db')['schemes']['layer_spec']['tables']['tiles_counter']
        # self.__tiles_counter_index = pg_credential.get('pg_db')['schemes']['layer_spec']['indexes'][
        #     'tiles_counter_id_seq']

        self.__mapproxy_scheme = pg_credential.get('pg_db')['schemes']['mapproxy_config']['name']
        self.__mapproxy_config_table = pg_credential.get('pg_db')['schemes']['mapproxy_config']['tables']['config']
        self.__mapproxy_config_index = pg_credential.get('pg_db')['schemes']['mapproxy_config']['indexes'][
            'config_id_seq']

        self.__catalog_manager_scheme = pg_credential.get('pg_db')['schemes']['raster_catalog_manager']['name']
        self.__catalog_records_table = pg_credential.get('pg_db')['schemes']['raster_catalog_manager']['tables'][
            'records']

    @property
    def get_class_params(self):
        params = {

            'job_manager': {
                'name': self.__job_manager_scheme,
                'jobs_table': self.__job_manager_jobs_table,
                'tasks_table': self.__job_manager_tasks_table
            },
            'mapproxy': {
                'name': self.__mapproxy_scheme,
                'mapproxy_config_table': self.__mapproxy_config_table,
                'mapproxy_config_index': self.__mapproxy_config_index
            }
        }

        return params

    # =========================================== read operation =======================================================
    def _get_connection_to_scheme(self, scheme_name):
        """
        This method create simple connection to specific scheme and return conn object
        """
        pg_conn = postgres.PGClass(host=self.__end_point_url,
                                   database=self.__db_name,
                                   user=self.__user,
                                   password=self.__password,
                                   scheme=scheme_name,
                                   port=self.__port)
        return pg_conn

    # =========================================== job manager ==========================================================

    def _get_jobs_by_criteria(self, product_id, product_version, product_type=None):
        """
        This method will return all jobs id by layer's criteria data
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will return all related jobs
        :return: list[str]
        """
        pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
        criteria = {
            'resourceId': product_id,
            'version': product_version
        }
        if product_type:
            criteria['productType'] = product_type
        job_ids = pg_conn.get_rows_by_keys(table_name=self.__job_manager_jobs_table,
                                           keys_values=criteria,
                                           return_as_dict=True
                                           )
        return job_ids

    def _get_tasks_by_id(self, job_id):
        """
        This method will return all jobs id by layer's criteria data
        :param job_id: list [job_id]
        :return: list[str]
        """
        pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)

        task_ids = pg_conn.get_by_n_argument(table_name=self.__job_manager_tasks_table,
                                             pk='jobId',
                                             pk_values=copy.deepcopy(job_id),
                                             column='id'
                                             )
        return task_ids

    def _delete_tasks_by_job_id(self, jobs):
        """
        This method will execute deletion query on task table according list of job ids
        :param jobs: list[str]
        :return:
        """
        results = []
        for job in jobs:
            pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
            resp = pg_conn.delete_row_by_id(self.__job_manager_tasks_table, "jobId", job)
            results.append(resp)
        return results

    def _delete_job_by_id(self, jobs):
        results = []
        for job in jobs:
            pg_conn = self._get_connection_to_scheme(self.__job_manager_scheme)
            resp = pg_conn.delete_row_by_id(self.__job_manager_jobs_table, "id", job)
            results.append(resp)
        return results

    def delete_job_task_by_layer(self, product_id, product_version, product_type=None):
        """
        This method will execute clean on layer's data in job_manager db's tables
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will remove all related jobs-tasks
        :return: dict => {state: bool, msg: dict}
        """

        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start Job manager DB cleaning for layer: [{product_id}:{product_version}]', length=140))
        try:
            # collect id's for job and task to delete
            job_ids = self._get_jobs_by_criteria(product_id, product_version, product_type)
            if not job_ids:
                _log.info(f'No jobs found for layer {product_id}:{product_version}')
                report = {'state': False, 'msg': f'No jobs found for layer {product_id}:{product_version}'}
            else:
                job_ids = [job_id['id'] for job_id in job_ids]
                _log.info(f'Found {len(job_ids)} jobs to delete')
                # collect all relevant id of tasks related to job list
                task_ids = self._get_tasks_by_id(job_ids)
                _log.info(f'Found {len(task_ids)} tasks to delete')
                # execute deletion
                task_deletion_result = self._delete_tasks_by_job_id(job_ids)
                job_deletion_result = self._delete_job_by_id(job_ids)

                report = {'state': True, 'msg': {
                    'job_to_delete': len(job_ids),
                    'task_to_delete': len(task_ids),
                    'deleted_job': job_deletion_result,
                    'deleted_task': task_deletion_result
                }}
                _log.info(f'\n delete results: [{json.dumps(report, indent=4)}]')

        except Exception as e:
            _log.error(f'Failed execute deletion on job manager db with error: [{str(e)}]')
            report = {'state': False, 'msg': f'Failed execute deletion on job manager db with error: [{str(e)}]'}
        _log.info('\n' + stringy.pad_with_minus('End of Job manager DB deletion', length=140) + '\n')
        return report

    # ============================================ layer spec ==========================================================
    # def get_tile_counter_rows(self, layer_id, target=None):
    #     """
    #     This method will query and find all records of tile counter (layer spec) for provided layer id
    #     :param layer_id: string => [product_id-product_version]
    #     :param target: string => default None and will search for all targets
    #     :return: list[dict] => records
    #     """
    #     pg_conn = self._get_connection_to_scheme(self.__layer_spec_scheme)
    #     criteria = {
    #         'layerId': layer_id
    #     }
    #     if target:
    #         criteria['target'] = target
    #
    #     tiles_counter_records = pg_conn.get_rows_by_keys(table_name=self.__tiles_counter_table,
    #                                                      keys_values=criteria,
    #                                                      return_as_dict=True
    #                                                      )
    #
    #     return tiles_counter_records

    # def _delete_tile_counter_by_layer(self, layer_id):
    #     pg_conn = self._get_connection_to_scheme(self.__layer_spec_scheme)
    #     results = pg_conn.delete_row_by_id(self.__tiles_counter_table, "layerId", layer_id)
    #     return results
    #
    # def delete_tile_counter_by_layer(self, product_id, product_version, target=None):
    #     """
    #     This method will execute clean on layers spec db and remove related tile count for provided layer
    #     :param product_id: string [layer's id]
    #     :param product_version: string [layer's version]
    #     :param target: string => default is None and will delete all layer's records
    #     :return: dict => {state: bool, msg: dict}
    #     """
    #     _log.info(
    #         "\n\n" + stringy.pad_with_stars(
    #             f'Start tile counter - layer spec DB cleaning for layer: [{product_id}-{product_version}]', length=140))
    #     layer_id = "-".join([product_id, product_version])
    #
    #     try:
    #         tiles_counter_records = self.get_tile_counter_rows(layer_id=layer_id, target=target)
    #         if not tiles_counter_records:
    #             _log.info(f'Records not found for layer: [{layer_id}]')
    #             report = {'state': False, 'msg': f'Records not found for layer: [{layer_id}]'}
    #         else:
    #             _log.info(f'Found {len(tiles_counter_records)} records to delete:\n'
    #                       f'{json.dumps(tiles_counter_records, indent=4)}')
    #             resp = self._delete_tile_counter_by_layer(layer_id)
    #             _log.info(f'Records deletion were executed with state: {resp}')
    #             report = {'state': True, 'msg': resp}
    #
    #     except Exception as e:
    #         _log.error(f'Failed tile counter DB clean up with error: [{str(e)}')
    #         report = {'state': False, 'msg': f'Failed tile counter DB clean up with error: [{str(e)}'}
    #
    #     _log.info('\n' + stringy.pad_with_minus('End of layer spec - tile counter DB deletion', length=140) + '\n')
    #     return report

    # ======================================= Catalog manager PYCSW ====================================================

    def get_raster_records_rows(self, product_id, product_version, product_type=None):
        """
        This method will query for all records on Catalog DB related to provided layer
        If product type not mentioned, it will return all types
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will return all records for layer
        :return: list[dict]
        """
        pg_conn = self._get_connection_to_scheme(self.__catalog_manager_scheme)
        criteria = {
            'product_id': product_id,
            'product_version': product_version
        }
        if product_type:
            criteria['product_type'] = product_type

        pycsw_records = pg_conn.get_rows_by_keys(table_name=self.__catalog_records_table,
                                                 keys_values=criteria,
                                                 return_as_dict=True
                                                 )

        return pycsw_records

    def _delete_pycsw_records(self, product_id, product_version, product_type=None):
        """
        This method will execute clean on pycsw records DB and remove related layer's records
        If product_type not provided, will remove all layer's records
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will remove all records for layer
        :return: dict => {state: bool, msg: dict}
        """
        records = self.get_raster_records_rows(product_id, product_version, product_type)
        results = []
        for record in records:
            pg_conn = self._get_connection_to_scheme(self.__catalog_manager_scheme)
            resp = pg_conn.delete_row_by_id(self.__catalog_records_table, "identifier", record['identifier'])
            results.append({record['product_type']: resp})
        return results

        _log.info('\n' + stringy.pad_with_minus('End of layer spec - tile counter DB deletion', length=140) + '\n')
        return report

    def delete_record_by_layer(self, product_id, product_version, product_type=None):
        """
        This method will execute clean on raster record DB and remove related records
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :param product_type: string [layer's type] -> optional, if not mentioned, will delete all records for layer
        :return: dict => {state: bool, msg: dict}
        """
        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start Catalog manager DB cleaning for layer: [{product_id}-{product_version}]', length=140))

        pycsw_records = self.get_raster_records_rows(product_id=product_id,
                                                     product_version=product_version,
                                                     product_type=product_type)
        if not pycsw_records:
            _log.info(f'Records not found for layer: [{product_id}-{product_version}]')
            report = {'state': False, 'msg': f'Records not found for layer: [{product_id}-{product_version}]'}

        else:
            _log.info(f'Found {len(pycsw_records)} records to delete:\n'
                      f'To see records run with log level - DEBUG')
            _log.debug(f'{json.dumps(pycsw_records, indent=4, sort_keys=True, default=str, ensure_ascii=False)}')
            resp = self._delete_pycsw_records(product_id, product_version, product_type)
            _log.info(f'Records deletion were executed with state: {resp}')
            report = {'state': True, 'msg': resp}

        _log.info('\n' + stringy.pad_with_minus('End of raster records pycsw DB deletion', length=140) + '\n')

        return report

    # ============================================= Mapproxy config ====================================================

    def _delete_mapproxy_config(self, layer_name, mapproxy_route):
        """
        This method will execute clean on mapproxy config DB and remove related layer's configs
        :param layer_names: list -> layer to delete
        :return: dict => {state: bool, msg: dict}
        """
        result = []
        # for layer_name in layer_names:
        layer_name = "-".join([layer_name, "OrthophotoBest"])
        resp = requests.delete(url=mapproxy_route, params={'layerNames': layer_name})

        # pg_conn = self._get_connection_to_scheme(self.__mapproxy_scheme)
        # resp = pg_conn.delete_by_json_key(table_name=self.__mapproxy_config_table,
        #                                   pk="data",
        #                                   canonic_keys=["caches"],
        #                                   value=layer_name)
        result.append({"layer": layer_name, "result": resp})
        return result

    def get_mapproxy_config(self, product_id, product_version=None):
        """
        This method will query for all configs of mapproxy on  DB related to provided layer
        :param product_id: string [layer's id]
        :param product_version: string [layer's version]
        :return: list[dict]
        """

        # ****** for compatibility -> user provide unused variable product_version
        pg_conn = self._get_connection_to_scheme(self.__mapproxy_scheme)
        criteria = ["caches"]

        # todo - may change to single layer type -> orthophoto on future sync version
        orthophoto = "-".join([product_id, "Orthophoto"])
        orthophotoBest = "-".join([product_id, "OrthophotoBest"])
        orthophotoHistory = "-".join([product_id, "OrthophotoHistory"])

        layers_names = [orthophoto, orthophotoHistory, orthophotoBest]
        layers = {layer: pg_conn.get_by_json_key(table_name=self.__mapproxy_config_table,
                                                 pk="data",
                                                 canonic_keys=criteria,
                                                 value=layer
                                                 ) for layer in layers_names}

        return layers

    # def remove_config_mapproxy(self, product_id, product_version, mapproxy_route):
    #     """
    #     This method will execute clean on raster mapproxy config DB and remove related configs with related layer
    #     :param product_id: string [layer's id]
    #     :param product_version: string [layer's version]
    #     :return: dict => {state: bool, msg: dict}
    #     """
    #     _log.info(
    #         "\n\n" + stringy.pad_with_stars(
    #             f'Start Mapproxy config DB cleaning for layer: [{product_id}-{product_version}]', length=140))
    #
    #     mapproxy_layers = self.get_mapproxy_config(product_id=product_id,
    #                                                product_version=product_version,
    #                                                )
    #     layer_to_delete = [layer for layer in mapproxy_layers.items() if len(layer[1]) > 0]
    #     if not len(layer_to_delete):
    #         _log.info(f'Not found configs for layer: [{product_id}]')
    #         report = {'state': False, 'msg': f'Not found configs for layer: [{product_id}]'}
    #
    #     else:
    #         _log.info(f'Found {len(layer_to_delete)} mapproxy configs to delete:\n'
    #                   f'To see configs run with log level - DEBUG')
    #         _log.debug(f'{json.dumps(layer_to_delete, indent=4, sort_keys=True, default=str, ensure_ascii=False)}')
    #         layer_names = [layer_names[0] for layer_names in layer_to_delete]
    #         resp = self._delete_mapproxy_config(layer_names=layer_names, mapproxy_route=mapproxy_route)
    #         _log.info(f'Configs deletion were executed with state: {json.dumps(resp, indent=4)}')
    #         report = {'state': True, 'msg': resp}
    #
    #     _log.info('\n' + stringy.pad_with_minus('End of Mapproxy configs DB deletion', length=140) + '\n')
    #
    #     return report

    # ============================================ Agent config ========================================================

    # def _delete_agent_db_records(self, product_id, product_version):
    #     """
    #     This method will execute clean on mapproxy config DB and remove related layer's configs
    #     :param product_id: str -> resource id
    #     :param product_version: str -> version
    #     :return: dict => {state: bool, msg: dict}
    #     """
    #     records = self.get_agent_db_record(product_id, product_version)
    #     result = []
    #     for record in records:
    #         pg_conn = self._get_connection_to_scheme(self.__discrete_agent_scheme)
    #         resp = pg_conn.delete_row_by_id(table_name=self.__discrete_agent_table,
    #                                         pk="directory",
    #                                         pk_value=record['directory']
    #                                         )
    #         res_per_iter = {"data": {'directory': record['directory'],
    #                                  'product_id': product_id,
    #                                  'product_version': product_version},
    #                         "result": resp}
    #         result.append(res_per_iter)
    #
    #     return result
    #
    # def get_agent_db_record(self, product_id, product_version=None, return_as_dict=True):
    #     """
    #     This method will query for all records of agent DB related to provided layer
    #     :param product_id: string [layer's id]
    #     :param product_version: string [layer's version]
    #     :param return_as_dict: bool -> The results as default returned as key value dict
    #     :return: list[dict]
    #     """
    #
    #     pg_conn = self._get_connection_to_scheme(self.__discrete_agent_scheme)
    #     layer_params = {
    #         'layerId': product_id,
    #         'version': product_version
    #     }
    #     agent_records = pg_conn.get_rows_by_keys(table_name=self.__discrete_agent_table,
    #                                              keys_values=layer_params,
    #                                              return_as_dict=return_as_dict
    #                                              )
    #
    #     return agent_records
    #
    # def remove_agent_db_record(self, product_id, product_version):
    #     """
    #    This method will execute clean on raster agent DB and remove related records with related layer ( watch
    #    directories cleanup
    #    :param product_id: string [layer's id]
    #    :param product_version: string [layer's version]
    #    :return: dict => {state: bool, msg: dict}
    #    """
    #     _log.info(
    #         "\n\n" + stringy.pad_with_stars(
    #             f'Start Agent DB cleaning for layer: [{product_id}-{product_version}]', length=140))
    #
    #     agent_records = self.get_agent_db_record(product_id=product_id,
    #                                              product_version=product_version,
    #                                              )
    #     if not len(agent_records):
    #         msg = f'Agent records Not found  for layer: [{product_id}-{product_version}]'
    #         _log.info(msg)
    #         report = {'state': False, 'msg': msg}
    #
    #     else:
    #         _log.info(f'Found {len(agent_records)} Agent records to delete:')
    #         _log.info(f'{json.dumps(agent_records, indent=4, sort_keys=True, default=str, ensure_ascii=False)}')
    #         resp = self._delete_agent_db_records(product_id=product_id, product_version=product_version)
    #         _log.info(f'Records deletion were executed with state: {json.dumps(resp, indent=4)}')
    #         report = {'state': True, 'msg': resp}
    #
    #     _log.info('\n' + stringy.pad_with_minus('End of Agent DB deletion', length=140) + '\n')
    #
    #     return report

    # ============================================ records ========================================================

    def get_tiles_path_convention(self, product_id):
        """
        This method return identifier and display path for tiles storage convention concatenation
        """
        pg_conn = self._get_connection_to_scheme(self.__catalog_manager_scheme)
        criteria = {
            'product_id': product_id
        }
        tiles_path_parameters = pg_conn.get_rows_by_keys(table_name=self.__catalog_records_table,
                                                         keys_values=criteria,
                                                         return_as_dict=True
                                                         )
        return tiles_path_parameters

    def parse_cleanup_data(self, cleanup_data):
        json_obj = {}
        for k, v in cleanup_data.items():
            json_obj["product_id"] = k
            json_obj["product_version"] = v

        return json_obj

    def get_daily_cleanup_data(self):
        """
        This method return json object that contains list of layers to be deleted
        """
        pg_conn = self._get_connection_to_scheme(self.__catalog_manager_scheme)
        data_to_delete = pg_conn.get_columns_by_like_statements(table_name=self.__catalog_records_table,
                                                                condition_param="or",
                                                                pk="product_id",
                                                                identifiers=["automation"],
                                                                columns="product_id, product_type, identifier, display_path")
        cleanup_format = []
        for layer in data_to_delete:
            layer_values = {"product_id": layer[0], "product_type": layer[1], "identifier": layer[2],
                            "display_path": layer[3]}
            cleanup_format.append(layer_values)
        return cleanup_format
