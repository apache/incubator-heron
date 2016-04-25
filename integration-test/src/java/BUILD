licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "core",
    srcs = glob(
        ["com/twitter/heron/integration_test/core/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        "//heron/proto:proto_topology_java",
        "//3rdparty/java:jackson",
        "@protobuf-java//jar",
        "@http-client//jar",
        "@http-core//jar",
    ],
)

java_library(
    name = "common",
    srcs = glob(
        ["com/twitter/heron/integration_test/common/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        "//3rdparty/java:hadoop-core",
        "//3rdparty/java:jackson",
        ":core"
    ],
)

java_library(
    name = "integration-tests-lib",
    srcs = glob(
       ["com/twitter/heron/integration_test/topology/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        ":common",
        ":core"
    ],
)

java_library(
    name = "local-integration-tests-lib",
    srcs = glob(
       ["com/twitter/heron/local_integration_test/topology/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        ":common",
        ":core"
    ],
)

java_binary(
    name = "integration-tests",
    srcs = glob(
       ["com/twitter/heron/integration_test/topology/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        "@commons-logging//jar",
        ":common",
        ":core"
    ],
)

java_binary(
    name = "local-integration-tests",
    srcs = glob(
       ["com/twitter/heron/local_integration_test/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        ":common",
        ":core"
    ],
)
