"""
This module manage the environment params for execution modules
"""
from cleanup_cli.wrappers import postgres, s3, fs


def set_pg_wrapper(config):
    """
    This module will return pg handler object
    :param config: pg params json
    :return: PostgresHandler object
    """
    pg_handler = postgres.PostgresHandler(config)
    return pg_handler


def set_s3_wrapper(config):
    """
    This module will return S3 handler object
    :param config: s3 params json
    :return: S3Handler object
    """
    s3_handler = s3.S3Handler(config)
    return s3_handler


def set_fs_wrapper(config):
    """
    This module will return FS handler object
    :param config: FS params json
    :return: FSHandler object
    """
    fs_handler = fs.FSHandler(config)
    return fs_handler
