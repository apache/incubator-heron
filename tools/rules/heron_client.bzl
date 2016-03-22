################################################################################
# Convenience macros for Heron client files
################################################################################
def heron_client_files():
    return heron_client_bin_files() + heron_client_lib_files() + heron_client_conf_files()

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

def heron_client_lib_scheduler_files():
    return [
        "//heron/newscheduler/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/schedulers/src/java:heron-aurora-scheduler",
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
    ]

def heron_client_lib_files():
    return [
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/logging:slf4j-api-java",
        "//3rdparty/logging:slf4j-jdk-java",
        "//3rdparty/logging:log4j-over-slf4j-java",
    ]
