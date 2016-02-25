package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = \
    heron_java_proto_files() + [
        "//heron/spi/src/java:common-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
    ]

localfs_deps_files = \
    common_deps_files + [
        "//heron/common/src/java:basics-java",
    ]
    
zookeeper_deps_files = \
    localfs_deps_files + [
        "//3rdparty/curator:curator-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ]

java_library(
    name = "statemgrs-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = zookeeper_deps_files, 
)

java_library(
    name = "localfs-statemgr-java",
    srcs = glob(
        ["**/FileSystemStateManager.java"],
        ["**/localfs/*.java"],
    ),
    deps = localfs_deps_files, 
)

java_library(
    name = "zookeeper-statemgr-java",
    srcs = glob(
        ["**/zookeeper/*.java"],
    ),
    deps = zookeeper_deps_files, 
)
