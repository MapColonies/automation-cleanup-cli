import json
import os
from build.lib.cleanup_cli.app import executers
from cleanup_cli.wrappers import env, connection

conf_dir = os.environ.get('CONF_FILE')
if conf_dir:
    resp = executers.load_json(conf_dir)
    if resp['state']:
        conf = resp['content']
        pg_handler = env.set_pg_wrapper(conf['pg_connection'])
        if conf['tiles_provider'].lower() == 's3':
            storage_handler = connection.StorageManager(env.set_s3_wrapper(conf['s3_connection']))
        data_to_clean = pg_handler.get_daily_cleanup_data()
        try:
            resp = executers.run_cleanup(data_to_clean, pg_handler, storage_handler)
        except Exception as e:
            resp = str(e)
            print(json.dumps(resp, indent=4))
