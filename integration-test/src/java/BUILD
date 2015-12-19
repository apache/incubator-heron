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
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/httpcomponents:http-client",
        "//3rdparty/httpcomponents:http-core",
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
        "//3rdparty/hadoop:hadoop-core",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
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
        "//3rdparty/logging:commons-logging-java",
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
