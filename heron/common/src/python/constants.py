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

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

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
RAM_FOR_STMGR = 1 * GB
DEFAULT_RAM_FOR_INSTANCE = 1 * GB
DEFAULT_DISK_PADDING_PER_CONTAINER = 12 * GB

LOGGING_DIRECTORY = 'heron.logging.directory'
MAX_LOG_FILES = 'heron.logging.maximum.files'
MAX_LOG_SIZE_MB = 'heron.logging.maximum.size.mb'

TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs"
TOPOLOGY_ACKING_ENABLED = "topology.acking"
METRICS_EXPORT_INTERVAL_SECS = "heron.metrics.export.interval.sec"

