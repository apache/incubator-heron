package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")
load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = 'scheduler-lib-java',
    srcs = glob([
        "**/service/**/*.java",
        "**/util/**/*.java",
    ]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",   # only for parsing version-*.json file. TODO(nbhagat): Remove it.
        "//3rdparty/guava:guava-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ] + heron_java_proto_files(),
)

java_library(
    name = 'scheduler-twitter-lib-java',
    srcs = glob([
        "**/twitter/*.java",
    ]),
    deps = [
        ":scheduler-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",   # only for parsing version-*.json file. TODO(nbhagat): Remove it.
        "//3rdparty/guava:guava-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ] + heron_java_proto_files(),
)

java_library(
    name='heron-local-scheduler-lib',
    srcs = glob(["**/local/*.java"]),
    deps = [
        ":scheduler-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
    ] + heron_java_proto_files(),
)

java_binary(
    name='heron-local-scheduler',
    srcs = glob(["**/local/*.java"]),
    deps = [
        ":scheduler-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
    ] + heron_java_proto_files(),
)

java_library(
    name='heron-aurora-scheduler-lib',
    srcs = glob(["**/aurora/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
    ] + heron_java_proto_files(),
)

java_binary(
    name='heron-aurora-scheduler',
    srcs = glob(["**/aurora/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
    ] + heron_java_proto_files(),
)

java_library(
    name = "heron-mesos-scheduler-lib",
    srcs = glob(
        ["**/mesos/**/*.java"],
    ),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
    ] + heron_java_proto_files(),
)

java_binary(
    name = "heron-mesos-scheduler",
    srcs = glob(
        ["**/mesos/**/*.java"],
    ),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
    ] + heron_java_proto_files(),
)

java_binary(
    name='scheduler-unshaded',
    srcs = glob(["**/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/spi/src/java:scheduler-spi-java",
        "//heron/common/src/java:basics-java",
        "//heron/statemgrs/src/java:statemgrs-java",
        "//heron/spi/src/java:statemgr-spi-java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ] + heron_java_proto_files(),
)

jarjar_binary(
    name = "heron-scheduler",
    src = ":scheduler-unshaded_deploy.jar",
    shade = "shade.conf",
)
