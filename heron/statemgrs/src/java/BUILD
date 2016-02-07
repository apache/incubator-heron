package(default_visibility = ["//visibility:public"])

load("/tools/rules/heron", "heron_java_proto_files")

java_library(
    name = "statemgrs-java",
    srcs = glob(
        ["**/*.java"],
    ),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/logging:logging-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ] + heron_java_proto_files(),
)

java_library(
    name = "dummy-statemgr-java",
    srcs = glob(
        ["**/dummy/*.java"],
    ),
    deps = [
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
    ] + heron_java_proto_files(),
)

java_library(
    name = "localfs-statemgr-java",
    srcs = glob(
        ["**/localfs/*.java"],
    ),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
    ] + heron_java_proto_files(),
)

java_library(
    name = "zookeeper-statemgr-java",
    srcs = glob(
        ["**/zookeeper/*.java"],
    ),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ] + heron_java_proto_files(),
)
