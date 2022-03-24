"""
This module provide the cli execution business logic and API
"""
import os
import json
from cleanup_cli.app import env


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


def run_cleanup(data_file):
    """
    This method will execute full cleanup according configuration
    :param data_file: layer to delete
    :return: dict -> results
    """
    tiles_provider = data_file.get('tiles_provider')
    if tiles_provider.lower() == 's3':
        s3_credential = data_file.get('s3_connection')
        if not s3_credential:
            return {'state': False, 'content': 'Missing key of [s3_connection] on data file'}
        s3_conn = env.set_s3_wrapper(s3_credential)

    elif tiles_provider.lower() == 'fs' or tiles_provider.lower() == 'nfs':
        print('TBD - Should be implemented')
        fs_credential = data_file.get('fs_credential')
        if not fs_credential:
            return {'state': False, 'content': 'Missing key of [fs_credential] on data file'}
    else:
        return {'state': False, 'content': 'Missing key of [tiles_provider] on data file'}

    pg_credential = data_file.get('pg_connection')
    if not pg_credential:
        return {'state': False, 'content': 'Missing key of [pg_connection] on data file'}

    pg_conn = env.set_pg_wrapper(pg_credential)



