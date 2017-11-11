################################################################################
# Convenience macros for Heron client files
################################################################################
def heron_client_bin_files():
    return [
        "//heron/tools/cli/src/python:heron",
        "//heron/tools/explorer/src/python:heron-explorer",
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

def heron_client_yarn_files():
    return [
        "//heron/config/src/yaml:conf-yarn-yaml",
    ]

def heron_client_lib_scheduler_files():
    return [
        "//heron/scheduler-core/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-aurora-scheduler",
        "//heron/schedulers/src/java:heron-slurm-scheduler",
        "//heron/schedulers/src/java:heron-yarn-scheduler",
        "//heron/schedulers/src/java:heron-mesos-scheduler",
        "//heron/schedulers/src/java:heron-marathon-scheduler",
        "//heron/schedulers/src/java:heron-kubernetes-scheduler",
        "//heron/schedulers/src/java:heron-ecs-scheduler",
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/packing/src/java:heron-binpacking-packing",
    ]

def heron_client_lib_packing_files():
    return [
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//heron/packing/src/java:heron-binpacking-packing",

    ]

def heron_client_lib_metricscachemgr_files():
    return [
        "//heron/metricscachemgr/src/java:heron-metricscachemgr",
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
        "//heron/uploaders/src/java:heron-scp-uploader",
        "//heron/uploaders/src/java:heron-gcs-uploader",
        "//heron/uploaders/src/java:heron-dlog-uploader",
    ]

def heron_client_lib_healthmgr_files():
    return [
        "//heron/healthmgr/src/java:heron-healthmgr",
    ]

def heron_client_lib_third_party_files():
    return [
        "@com_google_protobuf_protobuf_java//jar",
        "@org_slf4j_slf4j_api//jar",
        "@org_slf4j_slf4j_jdk14//jar",
    ]
