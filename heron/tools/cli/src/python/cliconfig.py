''' cliconfig.py '''
import os
import ConfigParser as configparser

from heron.common.src.python.utils.log import Log

CONFIG_SECTION = 'core'
PROP_SERVICE_URL = 'service_url'

valid_properties = {PROP_SERVICE_URL}


def get_config_directory():
  home = os.path.expanduser('~')
  return os.path.join(home, '.config', 'heron')


def get_cluster_config_file(cluster):
  return os.path.join(get_config_directory(), cluster)


def cluster_config(cluster):
  config = _cluster_config(cluster)
  if _config_has_property(config, PROP_SERVICE_URL):
    return {PROP_SERVICE_URL: config.get(CONFIG_SECTION, PROP_SERVICE_URL)}
  return dict()


def is_valid_property(prop):
  return prop in valid_properties


def set_property(cluster, prop, value):
  Log.debug("setting %s to %s for cluster %s", prop, value, cluster)
  if is_valid_property(prop):
    config = _cluster_config(cluster)
    config.set(CONFIG_SECTION, prop, value)
    _save_or_remove(config, get_cluster_config_file(cluster))
    return True

  return False


def unset_property(cluster, prop):
  if is_valid_property(prop):
    config = _cluster_config(cluster)
    if _config_has_property(config, prop):
      config.remove_option(CONFIG_SECTION, prop)
      _save_or_remove(config, get_cluster_config_file(cluster))
      return True

  return False


def _config_has_property(config, prop):
  return config.has_section(CONFIG_SECTION) and config.has_option('core', prop)


def _save_or_remove(config, cluster_config_file):
  if config.sections():
    Log.debug("saving config file: %s", cluster_config_file)
    with open(cluster_config_file, 'wb') as cf:
      config.write(cf)
  else:
    if os.path.isfile(cluster_config_file):
      try:
        os.remove(cluster_config_file)
      except OSError:
        pass


def _cluster_config(cluster):
  config = configparser.ConfigParser()
  cluster_config_file = get_cluster_config_file(cluster)
  if os.path.isfile(cluster_config_file):
    config.read(cluster_config_file)

  return config
