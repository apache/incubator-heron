################################################################################
# Convenience macros for Heron core files
################################################################################
def heron_core_files():
    return heron_core_bin_files() + heron_core_conf_files() + heron_core_lib_files()

def heron_core_bin_files():
    return [
        "//heron/executor/src/python:heron-executor",
        "//heron/shell/src/python:heron-shell",
        "//heron/stmgr/src/cpp:heron-stmgr",
        "//heron/tmaster/src/cpp:heron-tmaster",
        "//heron/instance/src/python/instance:heron-python-instance",
    ]

def heron_core_conf_files():
    return [
        "//heron/instance/src/java:aurora-logging-properties",
        "//heron/config/src/yaml:config-internals-yaml",
        "//heron/config/src/yaml:metrics-sinks-yaml",
    ]

def heron_core_lib_files():
    return heron_core_lib_scheduler_files() + \
        heron_core_lib_packing_files() + \
        heron_core_lib_metricsmgr_files() + \
        heron_core_lib_statemgr_files() + \
        heron_core_lib_instance_files()

def heron_core_lib_scheduler_files():
    return [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/schedulers/src/java:heron-marathon-scheduler",
    ]

def heron_core_lib_packing_files():
    return [
        "//heron/packing/src/java:heron-roundrobin-packing",
	"//heron/packing/src/java:heron-binpacking-packing"
    ]

def heron_core_lib_metricsmgr_files():
    return [
        "//heron/metricsmgr/src/java:heron-metricsmgr",
    ]

def heron_core_lib_statemgr_files():
    return [
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
    ]

def heron_core_lib_instance_files():
    return [
        "//heron/instance/src/java:heron-instance",
    ]
