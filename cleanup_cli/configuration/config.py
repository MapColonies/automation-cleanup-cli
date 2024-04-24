"""
This module will get initial outer configuration params for environments
"""
import json
from mc_automation_tools import common


def load_config():
    """This method will load config from file"""
    CONF_FILE = common.get_environment_variable('CONF_FILE', None)
    if not CONF_FILE:
        raise EnvironmentError('Should provide path for CONF_FILE')

    with open(CONF_FILE, 'r') as fp:
        conf = json.load(fp)
        return conf


