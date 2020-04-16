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

''' topology.py '''
import traceback
import uuid

from heron.common.src.python.utils.log import Log

from heronpy.api import api_constants

# pylint: disable=too-many-instance-attributes
class Topology(object):
  """
  Class Topology
    Contains all the relevant information about
    a topology that its state manager has.

    All this info is fetched from state manager in one go.

    The watches are the callbacks that are called
    when there is any change in the topology
    instance using set_physical_plan, set_execution_state,
    set_tmaster, and set_scheduler_location. Any other means of changing will
    not call the watches.
  """

  def __init__(self, name, state_manager_name):
    self.zone = None
    self.name = name
    self.state_manager_name = state_manager_name
    self.physical_plan = None
    self.packing_plan = None
    self.execution_state = None
    self.id = None
    self.cluster = None
    self.environ = None
    self.tmaster = None
    self.scheduler_location = None

    # A map from UUIDs to the callback
    # functions.
    self.watches = {}

  def register_watch(self, callback):
    """
    Returns the UUID with which the watch is
    registered. This UUID can be used to unregister
    the watch.
    Returns None if watch could not be registered.

    The argument 'callback' must be a function that takes
    exactly one argument, the topology on which
    the watch was triggered.
    Note that the watch will be unregistered in case
    it raises any Exception the first time.

    This callback is also called at the time
    of registration.
    """
    RETRY_COUNT = 5
    # Retry in case UID is previously
    # generated, just in case...
    for _ in range(RETRY_COUNT):
      # Generate a random UUID.
      uid = uuid.uuid4()
      if uid not in self.watches:
        Log.info("Registering a watch with uid: " + str(uid))
        try:
          callback(self)
        except Exception as e:
          Log.error("Caught exception while triggering callback: " + str(e))
          Log.debug(traceback.format_exc())
          return None
        self.watches[uid] = callback
        return uid
    return None

  def unregister_watch(self, uid):
    """
    Unregister the watch with the given UUID.
    """
    # Do not raise an error if UUID is
    # not present in the watches.
    Log.info("Unregister a watch with uid: " + str(uid))
    self.watches.pop(uid, None)

  def trigger_watches(self):
    """
    Call all the callbacks.
    If any callback raises an Exception,
    unregister the corresponding watch.
    """
    to_remove = []
    for uid, callback in list(self.watches.items()):
      try:
        callback(self)
      except Exception as e:
        Log.error("Caught exception while triggering callback: " + str(e))
        Log.debug(traceback.format_exc())
        to_remove.append(uid)

    for uid in to_remove:
      self.unregister_watch(uid)

  def set_physical_plan(self, physical_plan):
    """ set physical plan """
    if not physical_plan:
      self.physical_plan = None
      self.id = None
    else:
      self.physical_plan = physical_plan
      self.id = physical_plan.topology.id
    self.trigger_watches()

  def set_packing_plan(self, packing_plan):
    """ set packing plan """
    if not packing_plan:
      self.packing_plan = None
      self.id = None
    else:
      self.packing_plan = packing_plan
      self.id = packing_plan.id
    self.trigger_watches()

  # pylint: disable=no-self-use
  def get_execution_state_dc_environ(self, execution_state):
    """
    Helper function to extract dc and environ from execution_state.
    Returns a tuple (cluster, environ).
    """
    return (execution_state.cluster, execution_state.environ)

  def set_execution_state(self, execution_state):
    """ set exectuion state """
    if not execution_state:
      self.execution_state = None
      self.cluster = None
      self.environ = None
    else:
      self.execution_state = execution_state
      cluster, environ = self.get_execution_state_dc_environ(execution_state)
      self.cluster = cluster
      self.environ = environ
      self.zone = cluster
    self.trigger_watches()

  def set_tmaster(self, tmaster):
    """ set exectuion state """
    self.tmaster = tmaster
    self.trigger_watches()

  def set_scheduler_location(self, scheduler_location):
    """ set exectuion state """
    self.scheduler_location = scheduler_location
    self.trigger_watches()

  def num_instances(self):
    """
    Number of spouts + bolts
    """
    num = 0

    # Get all the components
    components = self.spouts() + self.bolts()

    # Get instances for each worker
    for component in components:
      config = component.comp.config
      for kvs in config.kvs:
        if kvs.key == api_constants.TOPOLOGY_COMPONENT_PARALLELISM:
          num += int(kvs.value)
          break

    return num

  def spouts(self):
    """
    Returns a list of Spout (proto) messages
    """
    if self.physical_plan:
      return list(self.physical_plan.topology.spouts)
    return []

  def spout_names(self):
    """
    Returns a list of names of all the spouts
    """
    return [component.comp.name for component in self.spouts()]

  def bolts(self):
    """
    Returns a list of Bolt (proto) messages
    """
    if self.physical_plan:
      return list(self.physical_plan.topology.bolts)
    return []

  def bolt_names(self):
    """
    Returns a list of names of all the bolts
    """
    return [component.comp.name for component in self.bolts()]

  def get_machines(self):
    """
    Get all the machines that this topology is running on.
    These are the hosts of all the stmgrs.
    """
    if self.physical_plan:
      stmgrs = list(self.physical_plan.stmgrs)
      return [s.host_name for s in stmgrs]
    return []

  def get_status(self):
    """
    Get the current state of this topology.
    The state values are from the topology.proto
    RUNNING = 1, PAUSED = 2, KILLED = 3
    if the state is None "Unknown" is returned.
    """
    status = None
    if self.physical_plan and self.physical_plan.topology:
      status = self.physical_plan.topology.state

    if status == 1:
      return "Running"
    elif status == 2:
      return "Paused"
    elif status == 3:
      return "Killed"
    else:
      return "Unknown"
