# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
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
  return {}


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
    with open(cluster_config_file, 'w', encoding='utf8') as cf:
      yaml.dump(config, cf, default_flow_style=False)
  else:
    if os.path.isfile(cluster_config_file):
      try:
        os.remove(cluster_config_file)
      except OSError:
        pass


def _cluster_config(cluster):
  config = {}
  cluster_config_file = get_cluster_config_file(cluster)
  if os.path.isfile(cluster_config_file):
    with open(cluster_config_file, 'r', encoding='utf8') as cf:
      config = yaml.safe_load(cf)

  return config
