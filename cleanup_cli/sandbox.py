from sync_tester.configuration import config
from sync_tester.cleanup_cli import postgres, s3

product_id = '2022_03_02T08_38_56Z_MAS_6_ORT_247557'
product_version = '4.0'

pg_config = config.PG_CONFIG_CORE_B
# print(pg_config)

pg_handler = postgres.PostgresHandler(pg_config)
#
pg_handler.delete_job_task_by_layer(product_id, product_version)
pg_handler.delete_tile_counter_by_layer(product_id, product_version)
pg_handler.delete_record_by_layer(product_id, product_version)
pg_handler.remove_config_mapproxy(product_id, product_version)
res = pg_handler.remove_agent_db_record(product_id, product_version)

s3_config = config._s3_credentials_b
s3_handler = s3.S3Handler(s3_config)
# s3_handler.is_object_exists(product_id+'/'+product_version)
# s3_handler.list_object(product_id+'/'+product_version)
# # s3_handler.list_object(product_id)
# # s3_handler.remove_layer_from_bucket(product_id)
s3_handler.clean_layer_from_s3(product_id)
