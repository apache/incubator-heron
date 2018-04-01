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
        "//heron/instance/src/python:heron-python-instance",
        "//heron/instance/src/cpp:heron-cpp-instance",
        "//heron/downloaders/src/shell:heron-downloader"
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
        heron_core_lib_instance_files() + \
        heron_core_lib_ckptmgr_files() + \
        heron_core_lib_statefulstorage_files() + \
        heron_core_lib_downloader_files()

def heron_core_lib_scheduler_files():
    return [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/schedulers/src/java:heron-marathon-scheduler",
        "//heron/schedulers/src/java:heron-kubernetes-scheduler",
        "//heron/schedulers/src/java:heron-nomad-scheduler"
    ]

def heron_core_lib_packing_files():
    return [
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/packing/src/java:heron-binpacking-packing"
    ]

def heron_core_lib_healthmgr_files():
    return [
        "//heron/healthmgr/src/java:heron-healthmgr",
    ]

def heron_core_lib_metricsmgr_files():
    return [
        "//heron/metricsmgr/src/java:heron-metricsmgr",
    ]

def heron_core_lib_metricscachemgr_files():
    return [
        "//heron/metricscachemgr/src/java:heron-metricscachemgr",
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

def heron_core_lib_ckptmgr_files():
    return [
        "//heron/ckptmgr/src/java:heron-ckptmgr",
    ]

def heron_core_lib_statefulstorage_files():
    return [
        "//heron/statefulstorages/src/java:heron-localfs-statefulstorage",
        "//heron/statefulstorages/src/java:heron-hdfs-statefulstorage",
        "//heron/statefulstorages/src/java:heron-dlog-statefulstorage",
    ]

def heron_core_lib_downloader_files():
  return [
    "//heron/downloaders/src/java:heron-downloader",
  ]
