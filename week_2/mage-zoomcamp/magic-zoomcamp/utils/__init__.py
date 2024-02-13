
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path

def get_config_by_env(env : str = 'default'):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    return ConfigFileLoader(config_path, env)
    