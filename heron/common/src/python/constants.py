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
''' constants.py '''
KB = 1024
MB = 1024 * KB
GB = 1024 * MB

MS_TO_SEC = 0.001
SEC_TO_NS = 1000000000

TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu"
TOPOLOGY_COMPONENT_JVMOPTS = "topology.component.jvmopts"
TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism"
TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram"
TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk"
TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap"
TOPOLOGY_STMGRS = "topology.stmgrs"
TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts"
TOPOLOGY_ADDITIONAL_CLASSPATH = "topology.additional.classpath"

TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs"
TOPOLOGY_ACKING_ENABLED = "topology.acking"
TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending"

RAM_FOR_STMGR = 1 * GB
DEFAULT_RAM_FOR_INSTANCE = 1 * GB
DEFAULT_DISK_PADDING_PER_CONTAINER = 12 * GB

LOGGING_DIRECTORY = 'heron.logging.directory'
MAX_LOG_FILES = 'heron.logging.maximum.files'
MAX_LOG_SIZE_MB = 'heron.logging.maximum.size.mb'

INSTANCE_ACK_BATCH_TIME_MS = "heron.instance.ack.batch.time.ms"
INSTANCE_EMIT_BATCH_TIME_MS = "heron.instance.emit.batch.time.ms"
INSTANCE_EMIT_BATCH_SIZE_BYTES = "heron.instance.emit.batch.size.bytes"
INSTANCE_EXECUTE_BATCH_TIME_MS = "heron.instance.execute.batch.time.ms"
INSTANCE_EXECUTE_BATCH_SIZE_BYTES = "heron.instance.execute.batch.size.bytes"

METRICS_EXPORT_INTERVAL_SECS = "heron.metrics.export.interval.sec"

INSTANCE_NW_WRITE_BATCH_SIZE_BYTES = "heron.instance.network.write.batch.size.bytes"
INSTANCE_NW_WRITE_BATCH_TIME_MS = "heron.instance.network.write.batch.time.ms"
INSTANCE_NW_READ_BATCH_SIZE_BYTES = "heron.instance.network.read.batch.size.bytes"
INSTANCE_NW_READ_BATCH_TIME_MS = "heron.instance.network.read.batch.time.ms"
INSTANCE_NW_OPTIONS_SOCKET_RECEIVED_BUFFER_SIZE_BYTES = \
  "heron.instance.network.options.socket.received.buffer.size.bytes"
INSTANCE_NW_OPTIONS_SOCKET_SEND_BUFFER_SIZE_BYTES = \
  "heron.instance.network.options.socket.send.buffer.size.bytes"
