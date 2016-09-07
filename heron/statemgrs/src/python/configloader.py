# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#!/usr/bin/env python2.7
''' configloader.py '''

import os
import re
import sys
import yaml

def load_state_manager_locations(cluster, state_manager_config_file='heron-conf/statemgr.yaml'):
  """ Reads configs to determine which state manager to use and converts them to state manager
  locations. Handles a subset of config wildcard substitution supported in the substitute method in
  com.twitter.heron.spi.common.Misc.java"""
  with open(state_manager_config_file, 'r') as stream:
    config = yaml.load(stream)

  home_dir = os.getenv('HOME')
  wildcards = {
      "~" : home_dir,
      "${HOME}" : home_dir,
      "${CLUSTER}" : cluster,
  }
  if os.getenv('JAVA_HOME'):
    wildcards["${JAVA_HOME}"] = os.getenv('JAVA_HOME')

  config = __replace(config, wildcards, state_manager_config_file)

  # need to convert from the format in statemgr.yaml to the format that the python state managers
  # takes. first, set reasonable defaults to local
  state_manager_location = {
      'type': 'file',
      'name': 'local',
      'tunnelhost': 'localhost',
      'rootpath': '~/.herondata/repository/state/local',
  }

  # then map the statemgr.yaml config keys to the python state manager location
  key_mappings = {
      'heron.statemgr.connection.string': 'hostport',
      'heron.statemgr.tunnel.host': 'tunnelhost',
      'heron.statemgr.root.path': 'rootpath',
  }
  for config_key in key_mappings:
    if config_key in config:
      state_manager_location[key_mappings[config_key]] = config[config_key]

  state_manager_class = config['heron.class.state.manager']
  if state_manager_class == 'com.twitter.heron.statemgr.zookeeper.curator.CuratorStateManager':
    state_manager_location['type'] = 'zookeeper'
    state_manager_location['name'] = 'zk'

  return [state_manager_location]

def __replace(config, wildcards, config_file):
  """For each kvp in config, do wildcard substitution on the values"""
  for config_key in config:
    config_value = config[config_key]
    original_value = config_value
    if isinstance(config_value, basestring):
      for token in wildcards:
        if wildcards[token]:
          config_value = config_value.replace(token, wildcards[token])
      found = re.findall(r'\${[A-Z_]+}', config_value)
      if found:
        raise ValueError("%s=%s in file %s contains unsupported or unset wildcard tokens: %s" %
                         (config_key, original_value, config_file, ", ".join(found)))
      config[config_key] = config_value
  return config

if __name__ == "__main__":
  # pylint: disable=pointless-string-statement
  """ helper main method used to verify config files, intended for manual verification only """
  if len(sys.argv) > 1:
    locations = load_state_manager_locations('local', sys.argv[1])
  else:
    locations = load_state_manager_locations('local')
  print "locations: %s" % locations
