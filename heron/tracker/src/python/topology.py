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

import logging
import traceback
import urllib2
import uuid

from heron.common.src.python import constants

LOG = logging.getLogger(__name__)

class Topology:
  """
  Class Topology
    Contains all the relevant information about
    a topology that its state manager has.

    All this info is fetched from state manager in one go.

    The watches are the callbacks that are called
    when there is any change in the topology
    instance using set_physical_plan, set_execution_state,
    and set_tmaster. Any other means of changing will
    not call the watches.
  """

  def __init__(self, name, state_manager_name):
    self.zone = None
    self.name = name
    self.state_manager_name = state_manager_name
    self.physical_plan = None
    self.execution_state = None
    self.id = None
    self.cluster = None
    self.environ = None
    self.tmaster = None

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
    for i in range(RETRY_COUNT):
      # Generate a random UUID.
      uid = uuid.uuid4()
      if uid not in self.watches:
        LOG.info("Registering a watch with uid: " + str(uid))
        try:
          callback(self)
        except Exception as e:
          LOG.error("Caught exception while triggering callback: " + str(e))
          traceback.print_exc()
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
    LOG.info("Unregister a watch with uid: " + str(uid))
    self.watches.pop(uid, None)

  def trigger_watches(self):
    """
    Call all the callbacks.
    If any callback raises an Exception,
    unregister the corresponding watch.
    """
    to_remove = []
    for uid, callback in self.watches.iteritems():
      try:
        callback(self)
      except Exception as e:
        LOG.error("Caught exception while triggering callback: " + str(e))
        traceback.print_exc()
        to_remove.append(uid)

    for uid in to_remove:
      self.unregister_watch(uid)

  def set_physical_plan(self, physical_plan):
    if not physical_plan:
      self.physical_plan = None
      self.id = None
    else:
      self.physical_plan = physical_plan
      self.id = physical_plan.topology.id
    self.trigger_watches()

  def get_execution_state_dc_environ(self, execution_state):
    """
    Helper function to extract dc and environ from execution_state.
    Returns a tuple (cluster, environ).
    """
    return (execution_state.cluster, execution_state.environ)

  def set_execution_state(self, execution_state):
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
    self.tmaster = tmaster
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
        if kvs.key == constants.TOPOLOGY_COMPONENT_PARALLELISM:
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
    return map(lambda component: component.comp.name, self.spouts())

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
    return map(lambda component: component.comp.name, self.bolts())

  def get_machines(self):
    """
    Get all the machines that this topology is running on.
    These are the hosts of all the stmgrs.
    """
    if self.physical_plan:
      stmgrs = list(self.physical_plan.stmgrs)
      return map(lambda s: s.host_name, stmgrs)
    return []

