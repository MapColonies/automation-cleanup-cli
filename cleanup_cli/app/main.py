"""
The main module to process automation cleanup
"""
import logging
import json
import os
from datetime import datetime
from cleanup_cli.app import executers
from cleanup_cli.wrappers import env, connection

menu = f"\nChoose one of the following Option:\n" \
       f"[1] - Configure cleanup data file (json)\n" \
       f"[2] - Show Data for deletion\n" \
       f"[3] - Execute Cleanup\n" \
       f"[4] - Configure environment file (json)\n" \
       f"[5] - Show environment conf (json)\n" \
       f"[0] - Exit"


def init_logger():
    log_mode = os.environ.get('LOG_MODE')  # Define if use debug+ mode logs -> default info+
    file_log = os.environ.get('LOG_TO_FILE')  # Define if write std out into file
    log_output_path = os.environ.get('LOG_OUTPUT_PATH', '/tmp')  # The directory to write log output

    # init logger
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    # define default handler to std out
    ch = logging.StreamHandler()

    # validate log level mode to define
    if not log_mode:
        ch.setLevel(logging.INFO)
    elif log_mode.lower() == 'warning':
        ch.setLevel(logging.WARNING)
    elif log_mode.lower() == 'debug':
        ch.setLevel(logging.DEBUG)
    else:
        print(f'unknown environment variable LOG_MODE: {log_mode}')

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # defining another handler to file on case it is been requested
    if file_log:
        log_file_name = ".".join([str(datetime.utcnow()), 'log'])  # pylint: disable=invalid-name
        fh = logging.FileHandler(os.path.join(log_output_path, log_file_name))
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(ch)


def exit_prog():
    opt = input('\n[9] -> for menu | exit with any other key: ')
    if opt == "9":
        print(menu)

    else:
        print('Will terminate running...')
        exit(0)


if __name__ == '__main__':
    init_logger()
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

    deletion_directory = None
    data_file = None
    print('This is MC data cleanup CLI')
    print('Multiple clean option of layers data on environment')
    print('*** Notice ***\n'
          'Should most provide directory of deletion data')

    run = True
    print(menu)
    while run:
        choice = input(">> ")
        if choice == "0":
            print('Will terminate running...')
            exit(0)

        elif choice == "1":
            deletion_directory = input('Please insert directory to deletion file: ')
            resp = executers.load_json(deletion_directory)
            if resp['state']:
                data_file = resp['content']
                print(f'Cleanup configured to {json.dumps(data_file, indent=3)}')
            else:
                data_file = None
                print(f'Failed load data to execute cleanup')
            exit_prog()

        elif choice == "2":
            if data_file:
                print(f'{json.dumps(data_file, indent=3)}')
            else:
                print(f'No data file loaded\n'
                      f'Insert valid json file for data [ chose option <2> ]')
            exit_prog()

        elif choice == "3":

            try:
                resp = executers.run_cleanup(data_file, pg_handler, storage_handler)
            except Exception as e:
                resp = str(e)

            print(json.dumps(resp, indent=4))
            exit_prog()

        elif choice == "4":
            conf_dir = input('Please insert directory to environment config file: ')
            resp = executers.load_json(conf_dir)
            if resp['state']:
                conf = resp['content']
                pg_handler = env.set_pg_wrapper(conf['pg_connection'])

                if conf['tiles_provider'].lower() == 's3':
                    storage_handler = connection.StorageManager(env.set_s3_wrapper(conf['s3_connection']))

                elif conf['tiles_provider'].lower() == 'fs' or conf['tiles_provider'].lower() == 'nfs':
                    storage_handler = connection.StorageManager(env.set_fs_wrapper(conf['fs_connection']))  # should be implemented

                print(f'Cleanup configured to environment {json.dumps(conf, indent=3)}')
            else:
                conf = None
                pg_handler = None
                storage_handler = None
                print(f'Failed load configuration of cleanup environment')
            exit_prog()

        elif choice == "5":
            if conf:
                print(f'{json.dumps(conf, indent=3)}')
            else:
                print(f'No data file loaded\n'
                      f'Insert valid json file for configuration [ chose option <4> ]')
            exit_prog()

        else:
            print(f'Wrong key value was insert [{choice}]')
            exit_prog()
