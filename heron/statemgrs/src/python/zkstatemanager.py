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

''' zkstatemanager.py '''
import sys
from six import reraise as raise_

from heron.proto.execution_state_pb2 import ExecutionState
from heron.proto.packing_plan_pb2 import PackingPlan
from heron.proto.physical_plan_pb2 import PhysicalPlan
from heron.proto.scheduler_pb2 import SchedulerLocation
from heron.proto.tmaster_pb2 import TMasterLocation
from heron.proto.topology_pb2 import Topology

from heron.statemgrs.src.python.log import Log as LOG
from heron.statemgrs.src.python.statemanager import StateManager
from heron.statemgrs.src.python.stateexceptions import StateException

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from kazoo.exceptions import NotEmptyError
from kazoo.exceptions import ZookeeperError

def _makehostportlist(hostportlist):
  return ','.join(["%s:%i" % hp for hp in hostportlist])

# pylint: disable=attribute-defined-outside-init
class ZkStateManager(StateManager):
  """
  State manager which connects to zookeeper and
  gets and sets states from there.
  """

  def __init__(self, name, hostportlist, rootpath, tunnelhost):
    super(ZkStateManager, self).__init__()
    self.name = name
    self.hostportlist = hostportlist
    self.tunnelhost = tunnelhost
    self.rootpath = rootpath

  # pylint: disable=no-self-use
  def _kazoo_client(self, hostportlist):
    """
    For Unit testing, replace this method to not
    Actually return a client
    """
    return KazooClient(hostportlist)

  def start(self):
    """ state Zookeeper """
    if self.is_host_port_reachable():
      self.client = self._kazoo_client(_makehostportlist(self.hostportlist))
    else:
      localhostports = self.establish_ssh_tunnel()
      self.client = self._kazoo_client(_makehostportlist(localhostports))
    self.client.start()

    def on_connection_change(state):
      """ callback to log """
      LOG.info("Connection state changed to: " + state)
    self.client.add_listener(on_connection_change)

  def stop(self):
    """ stop Zookeeper """
    self.client.stop()
    self.terminate_ssh_tunnel()

  # pylint: disable=function-redefined
  def get_topologies(self, callback=None):
    """ get topologies """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """Custom callback to get the topologies right now."""
        ret["result"] = data

    try:
      # Ensure the topology path exists. If a topology has never been deployed
      # then the path will not exist so create it and don't crash.
      # (fixme) add a watch instead of creating the path?
      self.client.ensure_path(self.get_topologies_path())

      self._get_topologies_with_watch(callback, isWatching)
    except NoNodeError:
      self.client.stop()
      path = self.get_topologies_path()
      raise_(StateException("Error required topology path '%s' not found" % (path),
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_topologies_with_watch(self, callback, isWatching):
    """
    Helper function to get topologies with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_topologies_path()
    if isWatching:
      LOG.info("Adding children watch for path: " + path)

    # pylint: disable=unused-variable
    @self.client.ChildrenWatch(path)
    def watch_topologies(topologies):
      """ callback to watch topologies """
      callback(topologies)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def get_topology(self, topologyName, callback=None):
    """ get topologies """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """Custom callback to get the topologies right now."""
        ret["result"] = data

    self._get_topology_with_watch(topologyName, callback, isWatching)

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_topology_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get pplan with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_topology_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-variable, unused-argument
    @self.client.DataWatch(path)
    def watch_topology(data, stats):
      """ watch topology """
      if data:
        topology = Topology()
        topology.ParseFromString(data)
        callback(topology)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def create_topology(self, topologyName, topology):
    """ crate topology """
    if not topology or not topology.IsInitialized():
      raise_(StateException("Topology protobuf not init properly",
                            StateException.EX_TYPE_PROTOBUF_ERROR), sys.exc_info()[2])

    path = self.get_topology_path(topologyName)
    LOG.info("Adding topology: {0} to path: {1}".format(
        topologyName, path))
    topologyString = topology.SerializeToString()
    try:
      self.client.create(path, value=topologyString, makepath=True)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while creating topology",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NodeExistsError:
      raise_(StateException("NodeExistsError while creating topology",
                            StateException.EX_TYPE_NODE_EXISTS_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while creating topology",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def delete_topology(self, topologyName):
    """ delete topology """
    path = self.get_topology_path(topologyName)
    LOG.info("Removing topology: {0} from path: {1}".format(
        topologyName, path))
    try:
      self.client.delete(path)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while deteling topology",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NotEmptyError:
      raise_(StateException("NotEmptyError while deleting topology",
                            StateException.EX_TYPE_NOT_EMPTY_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while deleting topology",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def get_packing_plan(self, topologyName, callback=None):
    """ get packing plan """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """ Custom callback to get the topologies right now. """
        ret["result"] = data

    self._get_packing_plan_with_watch(topologyName, callback, isWatching)

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_packing_plan_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get packing_plan with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_packing_plan_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-argument,unused-variable
    @self.client.DataWatch(path)
    def watch_packing_plan(data, stats):
      """ watch the packing plan for updates """
      if data:
        packing_plan = PackingPlan()
        packing_plan.ParseFromString(data)
        callback(packing_plan)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def get_pplan(self, topologyName, callback=None):
    """ get physical plan """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """
        Custom callback to get the topologies right now.
        """
        ret["result"] = data

    self._get_pplan_with_watch(topologyName, callback, isWatching)

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_pplan_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get pplan with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_pplan_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-variable, unused-argument
    @self.client.DataWatch(path)
    def watch_pplan(data, stats):
      """ invoke callback to watch physical plan """
      if data:
        pplan = PhysicalPlan()
        pplan.ParseFromString(data)
        callback(pplan)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def create_pplan(self, topologyName, pplan):
    """ create physical plan """
    if not pplan or not pplan.IsInitialized():
      raise_(StateException("Physical Plan protobuf not init properly",
                            StateException.EX_TYPE_PROTOBUF_ERROR), sys.exc_info()[2])

    path = self.get_pplan_path(topologyName)
    LOG.info("Adding topology: {0} to path: {1}".format(
        topologyName, path))
    pplanString = pplan.SerializeToString()
    try:
      self.client.create(path, value=pplanString, makepath=True)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while creating pplan",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NodeExistsError:
      raise_(StateException("NodeExistsError while creating pplan",
                            StateException.EX_TYPE_NODE_EXISTS_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while creating pplan",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def delete_pplan(self, topologyName):
    """ delete physical plan info """
    path = self.get_pplan_path(topologyName)
    LOG.info("Removing topology: {0} from path: {1}".format(
        topologyName, path))
    try:
      self.client.delete(path)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while deleting pplan",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NotEmptyError:
      raise_(StateException("NotEmptyError while deleting pplan",
                            StateException.EX_TYPE_NOT_EMPTY_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while deleting pplan",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def get_execution_state(self, topologyName, callback=None):
    """ get execution state """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """
        Custom callback to get the topologies right now.
        """
        ret["result"] = data

    self._get_execution_state_with_watch(topologyName, callback, isWatching)

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_execution_state_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get execution state with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_execution_state_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-variable, unused-argument
    @self.client.DataWatch(path)
    def watch_execution_state(data, stats):
      """ invoke callback to watch execute state """
      if data:
        executionState = ExecutionState()
        executionState.ParseFromString(data)
        callback(executionState)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def create_execution_state(self, topologyName, executionState):
    """ create execution state """
    if not executionState or not executionState.IsInitialized():
      raise_(StateException("Execution State protobuf not init properly",
                            StateException.EX_TYPE_PROTOBUF_ERROR), sys.exc_info()[2])

    path = self.get_execution_state_path(topologyName)
    LOG.info("Adding topology: {0} to path: {1}".format(
        topologyName, path))
    executionStateString = executionState.SerializeToString()
    try:
      self.client.create(path, value=executionStateString, makepath=True)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while creating execution state",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NodeExistsError:
      raise_(StateException("NodeExistsError while creating execution state",
                            StateException.EX_TYPE_NODE_EXISTS_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while creating execution state",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def delete_execution_state(self, topologyName):
    """ delete execution state """
    path = self.get_execution_state_path(topologyName)
    LOG.info("Removing topology: {0} from path: {1}".format(
        topologyName, path))
    try:
      self.client.delete(path)
      return True
    except NoNodeError:
      raise_(StateException("NoNodeError while deleting execution state",
                            StateException.EX_TYPE_NO_NODE_ERROR), sys.exc_info()[2])
    except NotEmptyError:
      raise_(StateException("NotEmptyError while deleting execution state",
                            StateException.EX_TYPE_NOT_EMPTY_ERROR), sys.exc_info()[2])
    except ZookeeperError:
      raise_(StateException("Zookeeper while deleting execution state",
                            StateException.EX_TYPE_ZOOKEEPER_ERROR), sys.exc_info()[2])
    except Exception:
      # Just re raise the exception.
      raise

  def get_tmaster(self, topologyName, callback=None):
    """ get tmaster """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """
        Custom callback to get the topologies right now.
        """
        ret["result"] = data

    self._get_tmaster_with_watch(topologyName, callback, isWatching)

    # The topologies are now populated with the data.
    return ret["result"]

  def _get_tmaster_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get pplan with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_tmaster_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-variable, unused-argument
    @self.client.DataWatch(path)
    def watch_tmaster(data, stats):
      """ invoke callback to watch tmaster """
      if data:
        tmaster = TMasterLocation()
        tmaster.ParseFromString(data)
        callback(tmaster)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching

  def get_scheduler_location(self, topologyName, callback=None):
    """ get scheduler location """
    isWatching = False

    # Temp dict used to return result
    # if callback is not provided.
    ret = {
        "result": None
    }
    if callback:
      isWatching = True
    else:
      def callback(data):
        """
        Custom callback to get the scheduler location right now.
        """
        ret["result"] = data

    self._get_scheduler_location_with_watch(topologyName, callback, isWatching)

    return ret["result"]

  def _get_scheduler_location_with_watch(self, topologyName, callback, isWatching):
    """
    Helper function to get scheduler location with
    a callback. The future watch is placed
    only if isWatching is True.
    """
    path = self.get_scheduler_location_path(topologyName)
    if isWatching:
      LOG.info("Adding data watch for path: " + path)

    # pylint: disable=unused-variable, unused-argument
    @self.client.DataWatch(path)
    def watch_scheduler_location(data, stats):
      """ invoke callback to watch scheduler location """
      if data:
        scheduler_location = SchedulerLocation()
        scheduler_location.ParseFromString(data)
        callback(scheduler_location)
      else:
        callback(None)

      # Returning False will result in no future watches
      # being triggered. If isWatching is True, then
      # the future watches will be triggered.
      return isWatching
