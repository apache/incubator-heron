################################################################################
# Convenience macros for Heron client files
################################################################################
def heron_client_bin_files():
    return [
        "//heron/cli/src/python:heron",
    ]

def heron_client_conf_files():
    return [
        "//heron/config/src/yaml:conf-yaml",
    ]

def heron_client_local_files():
    return [
        "//heron/config/src/yaml:conf-local-yaml",
    ]

def heron_client_aurora_files():
    return [
        "//heron/config/src/yaml:conf-aurora-yaml",
    ]

def heron_client_mesos_files():
    return [
        "//heron/config/src/yaml:conf-mesos-yaml",
    ]


def heron_client_lib_scheduler_files():
    return [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-aurora-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/packing/src/java:heron-roundrobin-packing",
    ]

def heron_client_lib_packing_files():
    return [
        "//heron/packing/src/java:heron-roundrobin-packing",
    ]

def heron_client_lib_statemgr_files():
    return [
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
    ]

def heron_client_lib_uploader_files():
    return [
        "//heron/uploaders/src/java:heron-null-uploader",
        "//heron/uploaders/src/java:heron-localfs-uploader",
        "//heron/uploaders/src/java:heron-s3-uploader",
        "//heron/uploaders/src/java:heron-hdfs-uploader",
    ]

def heron_client_lib_3rdparty_files():
    return [
        "@protobuf-java//jar",
        "@slf4j-api//jar",
        "@slf4j-jdk//jar",
        "@log4j-over-slf4j//jar",
    ]
