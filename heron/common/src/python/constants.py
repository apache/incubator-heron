KB = 1024
MB = 1024 * KB
GB = 1024 * MB

TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu"
TOPOLOGY_COMPONENT_JVMOPTS = "topology.component.jvmopts"
TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism"
TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram"
TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk"
TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap"
TOPOLOGY_STMGRS = "topology.stmgrs"
TOPOLOGY_WORKER_CHILDOPTS = "topology.worker.childopts"
TOPOLOGY_ADDITIONAL_CLASSPATH = "topology.additional.classpath"

# include stream mgr, metrics mgr or other daemon processes
RAM_FOR_DAEMON_PROCESSES = 2 * GB
DEFAULT_RAM_FOR_INSTANCE = 1 * GB
DEFAULT_DISK_PADDING_PER_CONTAINER = 12 * GB
