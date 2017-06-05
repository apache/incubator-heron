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
        "//third_party/java:jackson",
        "@commons_logging_commons_logging//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "@org_apache_httpcomponents_http_client//jar",
        "@org_apache_httpcomponents_http_core//jar",
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
        "//third_party/java:hadoop-core",
        "//third_party/java:jackson",
        "@commons_cli_commons_cli//jar",
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
        "@commons_cli_commons_cli//jar",
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
    name = "integration-tests-unshaded",
    srcs = glob(
       ["com/twitter/heron/integration_test/topology/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/storm/src/java:storm-compatibility-java",
        "@commons_cli_commons_cli//jar",
        ":common",
        ":core"
    ],
)

genrule(
    name = "integration-tests",
    srcs = [":integration-tests-unshaded_deploy.jar"],
    outs = ["integration-tests.jar"],
    cmd = "cp $< $@",
)

java_binary(
    name = "local-integration-tests-unshaded",
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

genrule(
    name = "local-integration-test",
    srcs = [":local-integration-tests-unshaded_deploy.jar"],
    outs = ["local-integration-tests.jar"],
    cmd = "cp $< $@",
)
