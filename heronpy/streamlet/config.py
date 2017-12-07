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

MB = 1024 * 1024
GB = MB * 1024

class Config(object):
  """Config is the way users configure the execution of the topology.
     Things like tuple delivery semantics, resources used, as well as
     user defined key/value pairs are passed on to the runner via
     this class.
  """
  ATMOST_ONCE = 1
  ATLEAST_ONCE = 2
  EFFECTIVELY_ONCE = 3

  def __init__(
      self,
      num_containers=2,
      container_ram=104857600,
      container_ram_mb=100,
      container_ram_gb=0.1,
      container_cpu=1.0,
      delivery_semantics=Config.ATMOST_ONCE,
      user_config={}):
    if config is not None and not isinstance(config, dict):
      raise RuntimeError("Config has to be of type dict")
    self._api_config = {}
    __set_num_containers(num_containers)

    for param in [container_ram, container_ram_mb, container_ram_gb]:
      if not isinstance(param, int):
        raise RuntimeError('All per-container RAM values must be ints')

    if container_ram is not None:
      __set_container_ram(container_ram)
    elif container_ram_mb is not None:
      __set_container_ram(container_ram_mb * MB)
    elif container_ram_gb is not None:
      __set_container_ram(ccontainer_ram_gb * GB)

    __set_container_cpu(container_cpu)
    __set_delivery_semantics(delivery_semantics)
    __set_user_config(user_config)

  def __set_delivery_semantics(self, semantics):
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

  def __set_num_containers(self, ncontainers):
    self._api_config[api_constants.TOPOLOGY_STMGRS] = int(ncontainers)

  def __set_container_ram(self, ram):
    self._api_config[api_constants.TOPOLOGY_CONTAINER_RAM_REQUESTED] = int(ram)
  
  def __set_container_cpu(self, cpu):
    self._api_config[api_constants.TOPOLOGY_CONTAINER_CPU_REQUESTED] = cpu

  def __set_user_config(self, user_config):
    if not isinstance(user_config_dict, dict):
      raise RuntimeError('User-specified topology config must be a dict')
    for k, v in user_config.iteritems():
      self._api_config[k] = v
