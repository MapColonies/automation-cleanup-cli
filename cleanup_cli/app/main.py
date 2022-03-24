"""
The main module to process automation cleanup
"""
import json
import os
from cleanup_cli.configuration import config
from cleanup_cli.app import env, executers

menu = f"\nChoose one of the following Option:\n" \
       f"[1] - Configure cleanup data file (json)\n" \
       f"[2] - Show Data for deletion\n" \
       f"[3] - Execute Cleanup\n" \
       f"[4] - Configure environment file (json)\n" \
       f"[5] - Show environment conf (json)\n" \
       f"[0] - Exit"


def exit_prog():
    opt = input('\n[9] -> for menu | exit with any other key: ')
    if opt == "9":
        print(menu)

    else:
        print('Will terminate running...')
        exit(0)


if __name__ == '__main__':
    conf_dir = os.environ.get('CONF_FILE')
    if conf_dir:
        resp = executers.load_json(conf_dir)
        if resp['state']:
            conf = resp['content']
            pg_handler = env.set_pg_wrapper(conf['pg_connection'])
            s3_handler = env.set_s3_wrapper(conf['s3_connection'])
        else:
            conf = 'Not defined'
            pg_handler = 'Not defined'
            s3_handler = 'Not defined'

    deletion_directory = 'Not defined'
    data_file = None
    print('This is MC data cleanup CLI')
    print('Multiple clean option of layers data on environment')

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
            conf_dir = input('Please insert directory to environment config file: ')
            try:
                resp = agent_api.stop_agent_watch()
            except Exception as e:
                resp = str(e)

            print(f'[{resp}]')
            exit_prog()

        elif choice == "4":
            conf_dir = input('Please insert directory to environment config file: ')
            resp = executers.load_json(conf_dir)
            if resp['state']:
                conf = resp['content']
                pg_handler = env.set_pg_wrapper(conf['pg_connection'])
                s3_handler = env.set_s3_wrapper(conf['s3_connection'])
                print(f'Cleanup configured to environment {json.dumps(conf, indent=3)}')
            else:
                conf = None
                pg_handler = 'undefined'
                s3_handler = 'undefined'
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



