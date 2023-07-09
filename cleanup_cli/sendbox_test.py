import os

from cleanup_cli.app import executers
from cleanup_cli.wrappers import postgres
from cleanup_cli.wrappers import env, connection

data_to_clean = os.environ.get('DELETE_FILE')
conf_dir = os.environ.get('CONF_FILE')
if conf_dir:
    resp = executers.load_json(conf_dir)
    if resp['state']:
        conf = resp['content']
        pg_handler = env.set_pg_wrapper(conf['pg_connection'])
        if conf['tiles_provider'].lower() == 's3':
            storage_handler = connection.StorageManager(env.set_s3_wrapper(conf['s3_connection']))
        elif conf['tiles_provider'].lower() == 'fs' or conf['tiles_provider'].lower() == 'nfs':
            storage_handler = connection.StorageManager(env.set_fs_wrapper(conf['fs_connection']))

else:
    conf = None
    pg_handler = None
    storage_handler = None

    results = {}

    tiles_storage_params = pg_handler.get_tiles_path_convention(product_id=layer_id)
    identifier = tiles_storage_params[0]['identifier']
    display_path = tiles_storage_params[0]['display_path']
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
