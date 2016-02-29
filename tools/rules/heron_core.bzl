################################################################################
# Convenience macros for Heron core files
################################################################################
def heron_core_files():
    return heron_core_bin_files() + heron_core_conf_files() + heron_core_lib_files()

def heron_core_bin_files():
    return [
        "//heron/controller/src/python:heron-controller",
        "//heron/executor/src/python:heron-executor",
        "//heron/shell/src/python:heron-shell",
        "//heron/stmgr/src/cpp:heron-stmgr",
        "//heron/tmaster/src/cpp:heron-tmaster",
    ]

def heron_core_conf_files():
    return [
        "//heron/instance/src/java:aurora-logging-properties",
        "//heron/config/src/yaml:config-internals-yaml",
        "//heron/config/src/yaml:metrics-sinks-yaml",
    ]

def heron_core_lib_files():
    return [
        "//heron/instance/src/java:heron-instance",
        "//heron/metricsmgr/src/java:heron-metricsmgr",
        "//heron/newscheduler/src/java:heron-scheduler",
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
    ]
