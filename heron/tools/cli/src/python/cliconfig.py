''' cliconfig.py '''
import os
import yaml

from heron.common.src.python.utils.log import Log

CLI_CONFIG = 'cli.yaml'
PROP_SERVICE_URL = 'service_url'

valid_properties = {PROP_SERVICE_URL}


def get_config_directory(cluster):
  home = os.path.expanduser('~')
  return os.path.join(home, '.config', 'heron', cluster)


def get_cluster_config_file(cluster):
  return os.path.join(get_config_directory(cluster), CLI_CONFIG)


def cluster_config(cluster):
  config = _cluster_config(cluster)
  if _config_has_property(config, PROP_SERVICE_URL):
    return {PROP_SERVICE_URL: config[PROP_SERVICE_URL]}
  return dict()


def is_valid_property(prop):
  return prop in valid_properties


def set_property(cluster, prop, value):
  Log.debug("setting %s to %s for cluster %s", prop, value, cluster)
  if is_valid_property(prop):
    config = _cluster_config(cluster)
    config[prop] = value
    _save_or_remove(config, cluster)
    return True

  return False


def unset_property(cluster, prop):
  if is_valid_property(prop):
    config = _cluster_config(cluster)
    if _config_has_property(config, prop):
      config.pop(prop, None)
      _save_or_remove(config, cluster)
      return True

  return False


def _config_has_property(config, prop):
  return prop in config


def _save_or_remove(config, cluster):
  cluster_config_file = get_cluster_config_file(cluster)
  if config:
    Log.debug("saving config file: %s", cluster_config_file)
    config_directory = get_config_directory(cluster)
    if not os.path.isdir(config_directory):
      os.makedirs(config_directory)
    with open(cluster_config_file, 'wb') as cf:
      yaml.dump(config, cf, default_flow_style=False)
  else:
    if os.path.isfile(cluster_config_file):
      try:
        os.remove(cluster_config_file)
      except OSError:
        pass


def _cluster_config(cluster):
  config = dict()
  cluster_config_file = get_cluster_config_file(cluster)
  if os.path.isfile(cluster_config_file):
    with open(cluster_config_file, 'r') as cf:
      config = yaml.load(cf)

  return config
