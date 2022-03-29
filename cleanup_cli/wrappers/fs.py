"""
This module will wrap all functionality interfacing with data on file system (tiles)
"""
import logging
import json
import os
import glob
import shutil

from mc_automation_tools.parse import stringy

_log = logging.getLogger('cleanup_cli.wrappers.fs')


class FSHandler:

    def __init__(self, fs_credential):
        """
        Initial s3 connection object that contain deployment relevant credits to query and use CRUD operation
        :param fs_credential: dict [json] -> sample:
        s3_credential =  {
                  "root_dir": string -> base dir on file system (mounted value),
                  "tiles_dir": string -> base tiles directory static dir,
                  "relative_dir": string -> specific layer relative dir,
                  }
        """
        self._root_dir = fs_credential.get('root_dir')
        self._tiles_dir = fs_credential.get('tiles_dir')
        self._relative_dir = fs_credential.get('relative_dir')
        self.full_tiles_dir = os.path.join(self._root_dir, self._tiles_dir, self._relative_dir)

    @property
    def get_class_params(self):
        params = {
            'root_dir': self._root_dir,
            'tiles_dir': self._tiles_dir,
            'relative_dir': self._relative_dir,
            'full_directory': self.full_tiles_dir
        }
        return params

    # def _submit_connection(self):
    #     """
    #     Helper method to create s3 connection client
    #     :return: s3 client
    #     """
    #     try:
    #         s3_conn = s3storage.S3Client(endpoint_url=self._end_point_url,
    #                                      aws_access_key_id=self.__access_key,
    #                                      aws_secret_access_key=self.__secret_key,
    #                                      )
    #         _log.debug(
    #             f'Connection to {self._end_point_url} created')
    #         return s3_conn
    #
    #     except Exception as e:
    #         err = f'Failed Connect {self._end_point_url} with error: [{str(e)}'
    #         _log.error(err)
    #         raise ConnectionError(err)

    def is_directory_exists(self, layer_dir):
        """
        This method will check if provided layer is exists on s3 bucket
        :param layer_dir: Full name of layer's directory on File System
        :return: bool
        """
        if not os.path.exists(layer_dir):
            return {'state': False, 'msg': f'Directory {layer_dir} not exists!'}

        return {'state': True, 'msg': f'Directory {layer_dir} was found'}

    def list_folder(self, layer_dir):
        """
        This method will list all files & folders to provided layer name (directory)
        :param layer_dir: Full name of layer's directory
        :return: list
        """

        files = glob.glob(layer_dir + '**/**', recursive=True)
        files = [f for f in files if os.path.isfile(f)]
        return files

    def remove_layer_from_fs(self, layer_dir):
        """
        This method empty and remove object and is internal items from bucket
        :param layer_dir:str -> represent the object key to be removed
        :return: dict -> {state: bool, msg: str, extra: dict -> response orig data}
        """

        try:
            shutil.rmtree(os.path.dirname(layer_dir))
            results = {'state': True, 'msg': f'Tiles directory {layer_dir} was removed'}
        except Exception as e:
            err = f'Failed connect and access data with error: [{str(e)}'
            _log.error(err)
            results = {'state': False, 'msg': f'Failed remove tiles from  {layer_dir}: [{str(e)}]'}

        return results

    def clean_layer(self, layer_name):
        """
        This method will validate exists of layer's tiles on s3 and remove tiles
        :param layer_name: represent the object key inside the configured bucket
        :return: dict
        """

        _log.info(
            "\n\n" + stringy.pad_with_stars(
                f'Start FS tiles cleaning for layer: [{layer_name}]', length=140))
        layer_dir = os.path.join(self._root_dir, self._tiles_dir, layer_name)
        if not layer_dir.endswith("/"):
            layer_dir = layer_dir + "/"
        directory_validation = self.is_directory_exists(layer_dir)
        if not directory_validation['state']:
            _log.info(f'Current directory not available with message: {directory_validation["msg"]}!')
            _log.info('\n' + stringy.pad_with_minus('End FS tiles cleaning', length=140) + '\n')
            return {'state': False,
                    'msg': f'Current directory {layer_dir} not available [{directory_validation["msg"]}]'}

        folder_content = self.list_folder(layer_dir)
        _log.info(f'Found {len(folder_content)} to remove\n'
                  f'To see content please run as debug log mode')
        _log.debug(f'Files included:\n'
                   f'{json.dumps(folder_content)}')

        results = self.remove_layer_from_fs(layer_dir)
        _log.info('\n' + stringy.pad_with_minus('End FS tiles cleaning', length=140) + '\n')

        return {'state': True, 'msg': f'Layer - {layer_name}, deleted {len(folder_content)} items',
                                      'extra': f'{results["msg"]}'}
