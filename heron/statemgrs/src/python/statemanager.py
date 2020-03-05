#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

''' statemanager.py '''
import abc

import socket
import subprocess
import six

from heron.statemgrs.src.python.log import Log as LOG

HERON_EXECUTION_STATE_PREFIX = "{0}/executionstate/"
HERON_PACKING_PLANS_PREFIX = "{0}/packingplans/"
HERON_PPLANS_PREFIX = "{0}/pplans/"
HERON_SCHEDULER_LOCATION_PREFIX = "{0}/schedulers/"
HERON_TMASTER_PREFIX = "{0}/tmasters/"
HERON_TOPOLOGIES_KEY = "{0}/topologies"

# pylint: disable=too-many-public-methods, attribute-defined-outside-init
class StateManager(six.with_metaclass(abc.ABCMeta)):
  """
  This is the abstract base class for state manager. It provides methods to get/set/delete various
  state from the state store. The getters accept an optional callback, which will watch for state
  changes of the object and invoke the callback when one occurs.
  """

  TIMEOUT_SECONDS = 5

  @property
  def name(self):
    return self.__name

  @name.setter
  def name(self, newName):
    self.__name = newName

  @property
  def hostportlist(self):
    return self.__hostportlist

  @hostportlist.setter
  def hostportlist(self, newHostportList):
    self.__hostportlist = newHostportList

  @property
  def rootpath(self):
    """ Getter for the path where the heron states are stored. """
    return self.__hostport

  @rootpath.setter
  def rootpath(self, newRootPath):
    """ Setter for the path where the heron states are stored. """
    self.__hostport = newRootPath

  @property
  def tunnelhost(self):
    """ Getter for the tunnelhost to create the tunnel if host is not accessible """
    return self.__tunnelhost

  @tunnelhost.setter
  def tunnelhost(self, newTunnelHost):
    """ Setter for the tunnelhost to create the tunnel if host is not accessible """
    self.__tunnelhost = newTunnelHost

  def __init__(self):
    self.tunnel = []

  def is_host_port_reachable(self):
    """
    Returns true if the host is reachable. In some cases, it may not be reachable a tunnel
    must be used.
    """
    for hostport in self.hostportlist:
      try:
        socket.create_connection(hostport, StateManager.TIMEOUT_SECONDS)
        return True
      except:
        LOG.info("StateManager %s Unable to connect to host: %s port %i"
                 % (self.name, hostport[0], hostport[1]))
        continue
    return False

  # pylint: disable=no-self-use
  def pick_unused_port(self):
    """ Pick an unused port. There is a slight chance that this wont work. """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    _, port = s.getsockname()
    s.close()
    return port

  def establish_ssh_tunnel(self):
    """
    Establish an ssh tunnel for each local host and port
    that can be used to communicate with the state host.
    """
    localportlist = []
    for (host, port) in self.hostportlist:
      localport = self.pick_unused_port()
      self.tunnel.append(subprocess.Popen(
          ('ssh', self.tunnelhost, '-NL127.0.0.1:%d:%s:%d' % (localport, host, port))))
      localportlist.append(('127.0.0.1', localport))
    return localportlist

  def terminate_ssh_tunnel(self):
    for tunnel in self.tunnel:
      tunnel.terminate()

  @abc.abstractmethod
  def start(self):
    """ If the state manager needs to connect to a remote host. """
    pass

  @abc.abstractmethod
  def stop(self):
    """ If the state manager had connected to a remote server, it would need to stop as well. """
    pass

  def get_topologies_path(self):
    return HERON_TOPOLOGIES_KEY.format(self.rootpath)

  def get_topology_path(self, topologyName):
    return HERON_TOPOLOGIES_KEY.format(self.rootpath) + "/" + topologyName

  def get_packing_plan_path(self, topologyName):
    return HERON_PACKING_PLANS_PREFIX.format(self.rootpath) + topologyName

  def get_pplan_path(self, topologyName):
    return HERON_PPLANS_PREFIX.format(self.rootpath) + topologyName

  def get_execution_state_path(self, topologyName):
    return HERON_EXECUTION_STATE_PREFIX.format(self.rootpath) + topologyName

  def get_tmaster_path(self, topologyName):
    return HERON_TMASTER_PREFIX.format(self.rootpath) + topologyName

  def get_scheduler_location_path(self, topologyName):
    return HERON_SCHEDULER_LOCATION_PREFIX.format(self.rootpath) + topologyName

  @abc.abstractmethod
  def get_topologies(self, callback=None):
    pass

  @abc.abstractmethod
  def get_topology(self, topologyName, callback=None):
    pass

  @abc.abstractmethod
  def create_topology(self, topologyName, topology):
    pass

  @abc.abstractmethod
  def delete_topology(self, topologyName):
    pass

  @abc.abstractmethod
  def get_packing_plan(self, topologyName, callback=None):
    """
    Gets the packing_plan for the topology.
    If the callback is provided,
    sets watch on the path and calls the callback
    with the new packing_plan.
    """
    pass

  @abc.abstractmethod
  def get_pplan(self, topologyName, callback=None):
    pass

  @abc.abstractmethod
  def create_pplan(self, topologyName, pplan):
    pass

  @abc.abstractmethod
  def delete_pplan(self, topologyName):
    pass

  @abc.abstractmethod
  def get_execution_state(self, topologyName, callback=None):
    pass

  @abc.abstractmethod
  def create_execution_state(self, topologyName, executionState):
    pass

  @abc.abstractmethod
  def delete_execution_state(self, topologyName):
    pass

  @abc.abstractmethod
  def get_tmaster(self, topologyName, callback=None):
    pass

  @abc.abstractmethod
  def get_scheduler_location(self, topologyName, callback=None):
    pass

  def delete_topology_from_zk(self, topologyName):
    """
    Removes the topology entry from:
    1. topologies list,
    2. pplan,
    3. execution_state, and
    """
    self.delete_pplan(topologyName)
    self.delete_execution_state(topologyName)
    self.delete_topology(topologyName)
