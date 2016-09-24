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

"""
This module includes a bunch of library functions
relevant for dealing with the topology structure
"""

import sets

from heron.common.src.python import constants
from heron.proto import topology_pb2

def get_topology_config(topology, key):
  """
  Helper function to get the value of key in topology config.
  Return None if the key is not present.
  """
  for kv in topology.topology_config.kvs:
    if kv.key == key:
      return kv.value
  return None

def get_component_parallelism(topology):
  """
  The argument is the proto object for topology.
  Returns a map of components to their parallelism.
  """
  cmap = {}
  components = list(topology.spouts) + list(topology.bolts)
  for component in components:
    component_name = component.comp.name
    for kv in component.comp.config.kvs:
      if kv.key == constants.TOPOLOGY_COMPONENT_PARALLELISM:
        cmap[component_name] = int(kv.value)
  return cmap

def get_nstmgrs(topology):
  """
  The argument is the proto object for topology.
  Returns the number of stream managers for the topology.
  This is equal to the number of containers.
  If not present, return 1 as default.
  """
  return int(get_topology_config(topology, constants.TOPOLOGY_STMGRS) or 1)

def get_instance_opts(topology):
  """
  The argument is the proto object for topology.
  Returns the topology worker child options.
  If not present, return empty string.
  """
  return str(get_topology_config(topology, constants.TOPOLOGY_WORKER_CHILDOPTS) or "")

def get_total_instances(topology):
  """
  The argument is the proto object for topology.
  Returns the total number of instances based on all parallelisms.
  """
  cmap = get_component_parallelism(topology)
  return sum(cmap.values())

def get_additional_classpath(topology):
  """
  The argument is the proto object for topology.
  Returns an empty string if no additional classpath specified
  """
  additional_classpath = str(get_topology_config(
      topology, constants.TOPOLOGY_ADDITIONAL_CLASSPATH) or "")
  return additional_classpath

def get_cpus_per_container(topology):
  """
  The argument is the proto object for topology.
  Calculate and return the CPUs per container.
  It can be calculated in two ways:
      1. It is passed in config.
      2. Allocate 1 CPU per instance in a container, and 1 extra for stmrs.
  """
  cpus = get_topology_config(topology, constants.TOPOLOGY_CONTAINER_CPU_REQUESTED)
  if not cpus:
    ninstances = get_total_instances(topology)
    nstmgrs = get_nstmgrs(topology)
    cpus = float(ninstances) / nstmgrs + 1 # plus one for stmgr
  return float(cpus)

def get_disk_per_container(topology):
  """
  The argument is the proto object for topology.
  Calculate and return the disk per container.
  It can be calculated in two ways:
      1. It is passed in config.
      2. Allocate 1 GB per instance in a container, and 12 GB extra for the rest.
  """
  disk = get_topology_config(topology, constants.TOPOLOGY_CONTAINER_DISK_REQUESTED)
  if not disk:
    ninstances = get_total_instances(topology)
    nstmgrs = get_nstmgrs(topology)
    # Round to the ceiling
    maxInstanceInOneContainer = (ninstances + nstmgrs - 1) / nstmgrs

    disk = maxInstanceInOneContainer * constants.GB + constants.DEFAULT_DISK_PADDING_PER_CONTAINER
  return disk

def get_ram_per_container(topology):
  """
  The argument is the proto object for topology.
  Calculate and return RAM per container for the topology based
  container rammap. Since rammap takes into account all the
  configs and cases, we just add some ram for stmgr and return it.
  """
  component_distribution = get_component_distribution(topology)
  rammap = get_component_rammap(topology)
  maxsize = 0
  for (_, container_items) in component_distribution.items():
    ramsize = 0
    for (component_name, _, _) in container_items:
      ramsize += int(rammap[component_name])
    if ramsize > maxsize:
      maxsize = ramsize
  return maxsize + constants.RAM_FOR_STMGR

def get_component_rammap(topology):
  """
  The argument is the proto object for topology.
  It is calculated based on the following priority:
      1. Form the user specified component rammap. If the rammap
         is complete, return it.
      2. If only some component rams are specified, assign them
         the default RAM, and return it.
      3. If no component RAM is specified, take the container ram requested
         and divide it equally among the max possible instances.
      4. If container RAM was not requested, assign the default RAM
         to all components.
  Returns a map from component name to ram in bytes
  """
  component_distribution = get_component_distribution(topology)
  components = list(topology.spouts) + list(topology.bolts)

  # first check if user has specified the rammap
  rammap = get_user_rammap(topology) or {}

  # Some or all of them are there, so assign default to those components
  # that are not specified.
  if len(rammap.keys()) > 0:
    for component in components:
      component_name = component.comp.name
      if component_name not in rammap:
        rammap[component_name] = constants.DEFAULT_RAM_FOR_INSTANCE
    return rammap

  max_instances_in_a_container = max(map(lambda x: len(x[1]), component_distribution.items()))

  # User has not specified any kind of rammap
  # We just find the ram needed for a container and divide
  # memory equally
  requested_ram_per_container = get_container_ram_requested(topology)
  if requested_ram_per_container != None:
    ram_for_jvms = requested_ram_per_container - constants.RAM_FOR_STMGR
    ram_per_instance = int(ram_for_jvms / max_instances_in_a_container)
    rammap = {}
    for component in components:
      component_name = component.comp.name
      rammap[component_name] = ram_per_instance
    return rammap

  # Nothing was specified.
  # The default is to allocate one 1gb per instance of all components
  ram_per_instance = int(constants.DEFAULT_RAM_FOR_INSTANCE)
  rammap = {}
  for component in components:
    component_name = component.comp.name
    rammap[component_name] = ram_per_instance
  return rammap

def strip_java_objects(topology):
  """
  The argument is the proto object for topology.
  The java objects are huge objects and topology
  is stripped off these objects so that it can be stored
  in zookeeper.
  """
  stripped_topology = topology_pb2.Topology()
  stripped_topology.CopyFrom(topology)
  components = list(stripped_topology.spouts) + list(stripped_topology.bolts)
  for component in components:
    if component.comp.HasField("serialized_object"):
      component.comp.ClearField("serialized_object")

  return stripped_topology

def get_component_distribution(topology):
  """
  The argument is the proto object for topology.
  Distribute components and their instances evenly across containers in a round robin fashion
  Return value is a map from container id to a list.
  Each element of a list is a tuple (component_name, global_task_id, component_index)
  This is essentially the physical plan of the topology.
  """
  containers = {}
  nstmgrs = get_nstmgrs(topology)
  for i in range(1, nstmgrs + 1):
    containers[i] = []
  index = 1
  global_task_id = 1
  for (component_name, ninstances) in get_component_parallelism(topology).items():
    component_index = 0
    for i in range(ninstances):
      containers[index].append((str(component_name), str(global_task_id), str(component_index)))
      component_index = component_index + 1
      global_task_id = global_task_id + 1
      index = index + 1
      if index == nstmgrs + 1:
        index = 1
  return containers

def get_user_rammap(topology):
  """
  The argument is the proto object for topology.
  Returns a dict of component to ram as specified by user.
  Expected user input is "comp1:42,comp2:24,..."
  where 42 and 24 represents the ram requested in bytes.
  Returns None if nothing is specified.
  """
  rammap = get_topology_config(topology, constants.TOPOLOGY_COMPONENT_RAMMAP)
  if rammap:
    rmap = {}
    for component_ram in rammap.split(","):
      component, ram = component_ram.split(":")
      rmap[component] = int(ram)
    return rmap
  return None

def get_container_ram_requested(topology):
  """
  The argument is the proto object for topology.
  Returns the container ram as requested by the user.
  Returns None if not specified.
  """
  ram = get_topology_config(topology, constants.TOPOLOGY_CONTAINER_RAM_REQUESTED)
  return int(ram) if ram else None

def get_component_jvmopts(topology):
  """
  The argument is the proto object for topology.
  Returns the jvm options if specified by user.
  """
  return str(get_topology_config(topology, constants.TOPOLOGY_COMPONENT_JVMOPTS) or "")

def get_topology_state_string(topology):
  """
  The argument is the proto object for topology.
  Returns the topology state as one of:
      1. RUNNING
      2. PAUSED
      3. KILLED
  """
  return topology_pb2.TopologyState.Name(topology.state)

# pylint: disable=too-many-return-statements, too-many-branches
def sane(topology):
  """ First check if topology name is ok """
  if topology.name == "":
    print "Topology name cannot be empty"
    return False
  if '.' in topology.name or '/' in topology.name:
    print "Topology name cannot contain . or /"
    return False

  component_names = set()
  for spout in topology.spouts:
    component_name = spout.comp.name
    if component_name in component_names:
      print component_name + " is duplicated twice"
      return False
    component_names.add(component_name)

  for bolt in topology.bolts:
    component_name = bolt.comp.name
    if component_name in component_names:
      print component_name + " is duplicated twice"
      return False
    component_names.add(component_name)

  ninstances = get_total_instances(topology)
  nstmgrs = get_nstmgrs(topology)
  if nstmgrs > ninstances:
    print "Number of containers are greater than number of instances"
    return False

  for kv in topology.topology_config.kvs:
    if kv.key == constants.TOPOLOGY_COMPONENT_RAMMAP:
      rammap = str(kv.value).split(',')
      for component_ram in rammap:
        component_and_ram = component_ram.split(':')
        if len(component_and_ram) != 2:
          print "TOPOLOGY_COMPONENT_RAMMAP is set incorrectly"
          return False
        if not component_and_ram[0] in component_names:
          print "TOPOLOGY_COMPONENT_RAMMAP is set incorrectly"
          return False

  # Now check if bolts are reading streams that some component emits
  all_outputs = sets.Set()
  for spout in topology.spouts:
    for outputstream in spout.outputs:
      all_outputs.add((outputstream.stream.id, outputstream.stream.component_name))
  for bolt in topology.bolts:
    for outputstream in bolt.outputs:
      all_outputs.add((outputstream.stream.id, outputstream.stream.component_name))
  for bolt in topology.bolts:
    for inputstream in bolt.inputs:
      if (inputstream.stream.id, inputstream.stream.component_name) not in all_outputs:
        print "Bolt " + bolt.comp.name + " has an input stream that no one emits"
        return False

  return True
