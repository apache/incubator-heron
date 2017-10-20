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
''' api_constants.py: defines api constants for topology config'''

####################################################################################################
###########################  Constants for topology configuration ##################################
####################################################################################################

class TopologyReliabilityMode(object):
  ATMOST_ONCE = "ATMOST_ONCE"
  ATLEAST_ONCE = "ATLEAST_ONCE"
  EFFECTIVELY_ONCE = "EFFECTIVELY_ONCE"

# Topology-specific options for the worker child process.
TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts"
# Per component jvm options.
TOPOLOGY_COMPONENT_JVMOPTS = "topology.component.jvmopts"
# How often a tick tuple from the "__system" component and "__tick" stream should be sent to tasks.
TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs"
# True if Heron should timeout messages or not. Default true. Meant to be used in unit tests.
TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts"
# When true, Heron will log every message that's emitted
TOPOLOGY_DEBUG = "topology.debug"
# The number of stmgr instances that should spin up to service this topology.
TOPOLOGY_STMGRS = "topology.stmgrs"
# The max amount of time given to the topology to fully process a message emitted by a spout.
TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs"
# The per component parallelism for a component in this topology.
TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism"
# The maximum number of tuples that can be pending on a spout task at any given time.
TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending"
# A list of task hooks that are automatically added to every spout and bolt in the topology.
TOPOLOGY_AUTO_TASK_HOOKS = "topology.auto.task.hooks"
# The serialization class that is used to serialize/deserialize tuples
TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname"
# Which reliability mode to run the topology?
# Valid ones are all the enums in TopologyReliabilityMode
TOPOLOGY_RELIABILITY_MODE = "topology.reliability.mode"
# What's the checkpoint interval for stateful topologies in seconds
TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL_SECONDS = "topology.stateful.checkpoint.interval.seconds"
# Boolean flag that says that the stateful topology should start from
# clean state, i.e. ignore any checkpoint state
TOPOLOGY_STATEFUL_START_CLEAN = "topology.stateful.start.clean"

# Number of cpu cores per container to be reserved for this topology.
TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu"
# Amount of ram per container to be reserved for this topology, in bytes.
TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram"
# Amount of disk per container to be reserved for this topology, in bytes.
TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk"
# Hint for max amount of ram per container to be reserved for this topology, in bytes.
TOPOLOGY_CONTAINER_MAX_CPU_HINT = "topology.container.max.cpu.hint"
# Hint for max amount of disk per container to be reserved for this topology, in bytes.
TOPOLOGY_CONTAINER_MAX_DISK_HINT = "topology.container.max.disk.hint"
# Padding percentage for this container.
TOPOLOGY_CONTAINER_PADDING_PERCENTAGE = "topology.container.padding.percentage"
# Amount of ram padding per container.
TOPOLOGY_CONTAINER_RAM_PADDING = "topology.container.ram.padding"

# Per component ram requirement.
TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap"
# Name of the topology, automatically set by Heron when the topology is submitted.
TOPOLOGY_NAME = "topology.name"
# Name of the team which owns this topology.
TOPOLOGY_TEAM_NAME = "topology.team.name"
# Email of the team which owns this topology.
TOPOLOGY_TEAM_EMAIL = "topology.team.email"
# Cap ticket (if filed) for the topology. If the topology is in prod, this has to be set or it
# cannot be deployed.
TOPOLOGY_CAP_TICKET = "topology.cap.ticket"
# Project name of the topology, to help us with tagging which topologies are part of which project.
TOPOLOGY_PROJECT_NAME = "topology.project.name"
# Any user defined classpath that needs to be passed to instances should be set in to config
# through this key.
TOPOLOGY_ADDITIONAL_CLASSPATH = "topology.additional.classpath"
