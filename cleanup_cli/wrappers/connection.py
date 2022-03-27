"""
This module will manage storage clean up
"""
from cleanup_cli.wrappers import s3, env


class StorageManager:
    def __init__(self, connector_handler=None):
        if not connector_handler:
            self.connection = None
            print('No tiles provider is configured!')

        self.connector_handler = connector_handler

    def remove_tiles(self, layer_name):
        """
        This method will remove according provided tiles storage source s3 / fs
        """
        try:
            response = self.connector_handler.clean_layer(layer_name)

        except Exception as e:
            response = {'state': False, 'msg': str(e), 'deleted': 'No deletion executed'}

        return response
