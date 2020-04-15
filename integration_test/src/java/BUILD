load("@rules_java//java:defs.bzl", "java_binary", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "test-data-files",
    srcs = glob(["org/apache/heron/integration_test/topology/**/*.json"]),
)

filegroup(
    name = "topology-test-data-files",
    srcs = glob(["org/apache/heron/integration_topology_test/topology/**/*.json"]),
)

java_library(
    name = "core",
    srcs = glob(
        ["org/apache/heron/integration_test/core/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/proto:proto_topology_java",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "//third_party/java:jackson",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:commons_logging_commons_logging",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "common",
    srcs = glob(
        ["org/apache/heron/integration_test/common/**/*.java"],
    ),
    deps = [
        ":core",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "//third_party/java:hadoop-core",
        "//third_party/java:jackson",
        "@maven//:commons_cli_commons_cli",
    ],
)

java_library(
    name = "integration-tests-lib",
    srcs = glob(
        ["org/apache/heron/integration_test/topology/**/*.java"],
    ),
    deps = [
        ":common",
        ":core",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:commons_cli_commons_cli",
    ],
)

java_library(
    name = "local-integration-tests-lib",
    srcs = glob(
        ["org/apache/heron/local_integration_test/topology/**/*.java"],
    ),
    deps = [
        ":common",
        ":core",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
    ],
)

java_binary(
    name = "integration-tests-unshaded",
    srcs = glob(
        ["org/apache/heron/integration_test/topology/**/*.java"],
    ),
    deps = [
        ":common",
        ":core",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:commons_cli_commons_cli",
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
        ["org/apache/heron/local_integration_test/**/*.java"],
    ),
    deps = [
        ":common",
        ":core",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
    ],
)

genrule(
    name = "local-integration-test",
    srcs = [":local-integration-tests-unshaded_deploy.jar"],
    outs = ["local-integration-tests.jar"],
    cmd = "cp $< $@",
)

java_library(
    name = "core-topology",
    srcs = glob(
        ["org/apache/heron/integration_topology_test/core/**/*.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/proto:proto_topology_java",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "//third_party/java:jackson",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:commons_logging_commons_logging",
        "@maven//:org_apache_httpcomponents_httpclient",
        "@maven//:org_apache_httpcomponents_httpcore",
    ],
)

java_library(
    name = "common_topology_test",
    srcs = glob(
        ["org/apache/heron/integration_topology_test/common/**/*.java"],
    ),
    deps = [
        ":core",
        ":core-topology",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "//third_party/java:hadoop-core",
        "//third_party/java:jackson",
        "@maven//:commons_cli_commons_cli",
    ],
)

java_library(
    name = "integration-topology-tests-lib",
    srcs = glob(
        ["org/apache/heron/integration_topology_test/topology/**/*.java"],
    ),
    deps = [
        ":common",
        ":common_topology_test",
        ":core",
        ":core-topology",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:commons_cli_commons_cli",
    ],
)

java_binary(
    name = "integration-topology-tests-unshaded",
    srcs = glob(
        ["org/apache/heron/integration_topology_test/topology/**/*.java"],
    ),
    deps = [
        ":common",
        ":common_topology_test",
        ":core",
        ":core-topology",
        "//heron/api/src/java:api-java",
        "//heron/api/src/java:api-java-low-level",
        "//storm-compatibility/src/java:storm-compatibility-java",
        "@maven//:com_googlecode_json_simple_json_simple",
        "@maven//:commons_cli_commons_cli",
    ],
)

genrule(
    name = "integration-topology-tests",
    srcs = [":integration-topology-tests-unshaded_deploy.jar"],
    outs = ["integration-topology-tests.jar"],
    cmd = "cp $< $@",
)
