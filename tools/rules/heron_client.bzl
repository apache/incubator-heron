################################################################################
# Convenience macros for Heron client files
################################################################################
def heron_client_files():
    return heron_client_bin_files() + heron_client_lib_files() + heron_client_conf_files()

def heron_client_bin_files():
    return [
        "//heron/cli3/src/python:heron-cli3",
    ]

def heron_client_conf_files():
    return [
        "//heron/config/src/yaml:conf-yaml",
    ]

def heron_client_local_files():
    return [
        "//heron/config/src/yaml:conf-local-yaml",
    ]

def heron_client_lib_files():
    return [
        "//heron/examples/src/java:heron-examples",
        "//heron/newscheduler/src/java:heron-scheduler",
        "//heron/schedulers/src/java:heron-local-scheduler",
        "//heron/uploaders/src/java:heron-localfs-uploader",
        "//heron/statemgrs/src/java:heron-zookeeper-statemgr",
        "//heron/statemgrs/src/java:heron-localfs-statemgr",
        "//heron/packing/src/java:heron-roundrobin-packing",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/logging:slf4j-api-java",
        "//3rdparty/logging:slf4j-jdk-java", 
        "//3rdparty/logging:log4j-over-slf4j-java",
    ]
