# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''config.py: module for defining config'''

import heronpy.api.api_constants as api_constants
from heronpy.streamlet.resources import Resources

class Config(object):
  """Config is the way users configure the execution of the topology.
     Things like tuple delivery semantics, resources used, as well as
     user defined key/value pairs are passed on to the runner via
     this class.
  """
  ATMOST_ONCE = 1
  ATLEAST_ONCE = 2
  EFFECTIVELY_ONCE = 3

  def __init__(self, config=None):
    if config is not None and not isinstance(config, dict):
      raise RuntimeError("Config has to be of type dict")
    self._api_config = config
    if self._api_config is None:
      self._api_config = {}

  def set_delivery_semantics(self, semantics):
    if semantics == Config.ATMOST_ONCE:
      self._api_config[api_constants.TOPOLOGY_RELIABILITY_MODE] =\
               api_constants.TopologyReliabilityMode.ATMOST_ONCE
    elif semantics == Config.ATLEAST_ONCE:
      self._api_config[api_constants.TOPOLOGY_RELIABILITY_MODE] =\
               api_constants.TopologyReliabilityMode.ATLEAST_ONCE
    elif semantics == Config.EFFECTIVELY_ONCE:
      self._api_config[api_constants.TOPOLOGY_RELIABILITY_MODE] =\
               api_constants.TopologyReliabilityMode.EFFECTIVELY_ONCE
    else:
      raise RuntimeError("Unknown Topology delivery semantics %s" % str(semantics))

  def set_num_containers(self, ncontainers):
    self._api_config[api_constants.TOPOLOGY_STMGRS] = int(ncontainers)

  def set_container_resources(self, resources):
    if not isinstance(resources, Resources):
      raise RuntimeError("container resources have to be of type Resources")
    self._api_config[api_constants.TOPOLOGY_CONTAINER_CPU_REQUESTED] = resources.get_cpu()
    self._api_config[api_constants.TOPOLOGY_CONTAINER_RAM_REQUESTED] = resources.get_ram()

  def set_user_config(self, key, value):
    self._api_config[key] = value
