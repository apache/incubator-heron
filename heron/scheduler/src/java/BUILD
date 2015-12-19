package(default_visibility = ["//visibility:public"])

java_library(
    name = 'scheduler-lib-java',
    srcs = glob([
        "**/api/**/*.java",
        "**/service/**/*.java",
        "**/util/**/*.java",
    ]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",   # only for parsing version-*.json file. TODO(nbhagat): Remove it.
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ],
)

java_library(
    name = 'scheduler-twitter-lib-java',
    srcs = glob([
        "**/twitter/*.java",
    ]),
    deps = [
        ":scheduler-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",   # only for parsing version-*.json file. TODO(nbhagat): Remove it.
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ],
)

java_library(
    name='heron-local-scheduler-lib',
    srcs = glob(["**/local/*.java"]),
    deps = [
        ":scheduler-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_binary(
    name='heron-local-scheduler',
    srcs = glob(["**/local/*.java"]),
    deps = [
        ":scheduler-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_library(
    name='heron-aurora-scheduler-lib',
    srcs = glob(["**/aurora/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
    resources = [
        "heron.aurora",
        "heron_controller.aurora",
    ],
)

java_binary(
    name='heron-aurora-scheduler',
    srcs = glob(["**/aurora/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
    resources = [
        "heron.aurora",
        "heron_controller.aurora",
    ],
)

java_library(
    name = "heron-mesos-scheduler-lib",
    srcs = glob(
        ["**/mesos/**/*.java"],
    ),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_binary(
    name = "heron-mesos-scheduler",
    srcs = glob(
        ["**/mesos/**/*.java"],
    ),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/api/src/java:api-java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/protobuf:protobuf-java",
    ],
)

java_binary(
    name='heron-scheduler',
    srcs = glob(["**/*.java"]),
    deps = [
        ":scheduler-lib-java",
        ":scheduler-twitter-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:base-lib-java",
        "//heron/state/src/java:state-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_execution_state_java",
        "//heron/proto:proto_scheduler_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/mesos:mesos-java",
        "//3rdparty/guava:guava-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/curator:curator-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/zookeeper:zookeeper-java",
    ],
    resources = [
        "heron.aurora",
        "heron_controller.aurora",
    ],
)
