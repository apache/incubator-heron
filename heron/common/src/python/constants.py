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
''' constants.py: defines constants for generic purposes and for topology and system config'''

##### Generic constants #####

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

MS_TO_SEC = 0.001
SEC_TO_NS = 1000000000
NS_TO_MS = 0.000001

RAM_FOR_STMGR = 1 * GB
DEFAULT_RAM_FOR_INSTANCE = 1 * GB
DEFAULT_DISK_PADDING_PER_CONTAINER = 12 * GB

####################################################################################################
###########################  Constants for topology configuration ##################################
####################################################################################################

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
# How many executors to spawn for ackers.
TOPOLOGY_ENABLE_ACKING = "topology.acking"

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


####################################################################################################
#############################  Constants for System configuration ##################################
####################################################################################################

# The relative path to the logging directory
HERON_LOGGING_DIRECTORY = 'heron.logging.directory'
# The maximum log file size in MB
HERON_LOGGING_MAXIMUM_SIZE_MB = "heron.logging.maximum.size.mb"
# The maximum number of log files
HERON_LOGGING_MAXIMUM_FILES = "heron.logging.maximum.files"
# The threshold level to log error
HERON_LOGGING_ERR_THRESHOLD = "heron.logging.err.threshold"
# The interval in seconds to get and reset the system metrics.
HERON_METRICS_EXPORT_INTERVAL_SEC = "heron.metrics.export.interval.sec"

####################################################################################################
############################### Sytem config: Instance related #####################################
####################################################################################################

# The queue capacity (num of items) in bolt for buffer packets to read from stream manager
INSTANCE_INTERNAL_BOLT_READ_QUEUE_CAPACITY = "heron.instance.internal.bolt.read.queue.capacity"
# The queue capacity (num of items) in bolt for buffer packets to write to stream manager
INSTANCE_INTERNAL_BOLT_WRITE_QUEUE_CAPACITY = "heron.instance.internal.bolt.write.queue.capacity"
# The queue capacity (num of items) in spout for buffer packets to read from stream manager
INSTANCE_INTERNAL_SPOUT_READ_QUEUE_CAPACITY = "heron.instance.internal.spout.read.queue.capacity"
# The queue capacity (num of items) in spout for buffer packets to write to stream manager
INSTANCE_INTERNAL_SPOUT_WRITE_QUEUE_CAPACITY = "heron.instance.internal.spout.write.queue.capacity"
# The queue capacity (num of items) for metrics packets to write to metrics manager
INSTANCE_INTERNAL_METRICS_WRITE_QUEUE_CAPACITY = \
  "heron.instance.internal.metrics.write.queue.capacity"

# Time based, the maximum batch time in ms for instance to read from stream manager per attempt
INSTANCE_NETWORK_READ_BATCH_TIME_MS = "heron.instance.network.read.batch.time.ms"
# Size based, the maximum batch size in bytes to read from stream manager
INSTANCE_NETWORK_READ_BATCH_SIZE_BYTES = "heron.instance.network.read.batch.size.bytes"
# Time based, the maximum batch time in ms for instance to write to stream manager per attempt
INSTANCE_NETWORK_WRITE_BATCH_TIME_MS = "heron.instance.network.write.batch.time.ms"
# Size based, the maximum batch size in bytes to write to stream manager
INSTANCE_NETWORK_WRITE_BATCH_SIZE_BYTES = "heron.instance.network.write.batch.size.bytes"
# The maximum socket's received buffer size in bytes of instance's network options
INSTANCE_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES = \
  "heron.instance.network.options.socket.received.buffer.size.bytes"
# The maximum socket's send buffer size in bytes
INSTANCE_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES = \
  "heron.instance.network.options.socket.send.buffer.size.bytes"

# The maximum # of data tuple to batch in a HeronDataTupleSet protobuf
INSTANCE_SET_DATA_TUPLE_CAPACITY = "heron.instance.set.data.tuple.capacity"
# The maximum size in bytes of data tuple to batch in a HeronDataTupleSet protobuf
INSTANCE_SET_DATA_TUPLE_SIZE_BYTES = "heron.instance.set.data.tuple.size.bytes"

# The maximum # of control tuple to batch in a HeronControlTupleSet protobuf
INSTANCE_SET_CONTROL_TUPLE_CAPACITY = "heron.instance.set.control.tuple.capacity"
# The maximum time in ms for an spout to do acknowledgement per attempt
INSTANCE_ACK_BATCH_TIME_MS = "heron.instance.ack.batch.time.ms"
# The maximum time in ms for an spout instance to emit tuples per attempt
INSTANCE_EMIT_BATCH_TIME_MS = "heron.instance.emit.batch.time.ms"
# The maximum batch size in bytes for an spout instance to emit tuples per attempt
INSTANCE_EMIT_BATCH_SIZE_BYTES = "heron.instance.emit.batch.size.bytes"
# The maximum time in ms for an bolt instance to execute tuples per attempt
INSTANCE_EXECUTE_BATCH_TIME_MS = "heron.instance.execute.batch.time.ms"
# The maximum batch size in bytes for an bolt instance to execute tuples per attempt
INSTANCE_EXECUTE_BATCH_SIZE_BYTES = "heron.instance.execute.batch.size.bytes"

# The time interval for an instance to check the state change, for instance,
# the interval a spout using to check whether activate/deactivate is invoked
INSTANCE_STATE_CHECK_INTERVAL_SEC = "heron.instance.state.check.interval.sec"
# The time to wait before the instance exits forcibly when uncaught exception happens
INSTANCE_FORCE_EXIT_TIMEOUT_MS = "heron.instance.force.exit.timeout.ms"
# Interval in seconds to reconnect to the stream manager
INSTANCE_RECONNECT_STREAMMGR_INTERVAL_SEC = "heron.instance.reconnect.streammgr.interval.sec"
# Interval in seconds to reconnect to the metrics manager
INSTANCE_RECONNECT_METRICSMGR_INTERVAL_SEC = "heron.instance.reconnect.metricsmgr.interval.sec"
# Interval in seconds to sample a system metric, for instance, jvm used memory.
INSTANCE_METRICS_SYSTEM_SAMPLE_INTERVAL_SEC = "heron.instance.metrics.system.sample.interval.sec"

# The lookForTimeout Interval in spout instance will be timeoutInSeconds / NBUCKETS
# For instance, if a tuple's timeout is 30s, and NBUCKETS is 10
# The spout instance will check whether there are timeout tuples every 3 seconds
INSTANCE_ACKNOWLEDGEMENT_NBUCKETS = "heron.instance.acknowledgement.nbuckets"

# The interval for different threads to attempt to fetch physical plan from SingletonRegistry
INSTANCE_SLAVE_FETCH_PPLAN_INTERVAL_SEC = "heron.instance.slave.fetch.pplan.interval.sec"
# The expected size on read queue in bolt
INSTANCE_TUNING_EXPECTED_BOLT_READ_QUEUE_SIZE \
  = "heron.instance.tuning.expected.bolt.read.queue.size"
# The expected size on write queue in bolt
INSTANCE_TUNING_EXPECTED_BOLT_WRITE_QUEUE_SIZE \
  = "heron.instance.tuning.expected.bolt.write.queue.size"
# The expected size on read queue in spout
INSTANCE_TUNING_EXPECTED_SPOUT_READ_QUEUE_SIZE \
  = "heron.instance.tuning.expected.spout.read.queue.size"
# The exepected size on write queue in spout
INSTANCE_TUNING_EXPECTED_SPOUT_WRITE_QUEUE_SIZE \
  = "heron.instance.tuning.expected.spout.write.queue.size"
# The expected size on metrics write queue
INSTANCE_TUNING_EXPECTED_METRICS_WRITE_QUEUE_SIZE \
  = "heron.instance.tuning.expected.metrics.write.queue.size"

# During dynamically tuning, the weight of new sample size to calculate
# the available capacity of queue.
# We use Exponential moving average: En = (1-w) * En-1 + w * An
# http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
INSTANCE_TUNING_CURRENT_SAMPLE_WEIGHT = "heron.instance.tuning.current.sample.weight"

# Interval in ms to tune the size of in &amp; out data queue in instance
INSTANCE_TUNING_INTERVAL_MS = "heron.instance.tuning.interval.ms"

####################################################################################################
########################### Sytem config: Metrics Manager related ##################################
####################################################################################################

# Time based, the maximum batch time in ms for instance to read from socket per attempt
METRICSMGR_NETWORK_READ_BATCH_TIME_MS = "heron.metricsmgr.network.read.batch.time.ms"
# Size based,the maximum batch size in bytes to read from socket
METRICSMGR_NETWORK_READ_BATCH_SIZE_BYTES = "heron.metricsmgr.network.read.batch.size.bytes"
# Time based, the maximum batch time in ms to write to socket
METRICSMGR_NETWORK_WRITE_BATCH_TIME_MS = "heron.metricsmgr.network.write.batch.time.ms"
# Size based, the maximum batch size in bytes to write to socket
METRICSMGR_NETWORK_WRITE_BATCH_SIZE_BYTES = "heron.metricsmgr.network.write.batch.size.bytes"
# The maximum socket's received buffer size in bytes
METRICSMGR_NETWORK_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES \
  = "heron.metricsmgr.network.options.socket.received.buffer.size.bytes"
# The maximum socket's send buffer size in bytes
METRICSMGR_NETWORK_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES \
  = "heron.metricsmgr.network.options.socket.send.buffer.size.bytes"
