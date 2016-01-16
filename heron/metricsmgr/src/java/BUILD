licenses(["notice"])

package(default_visibility = ["//visibility:public"])

load("/tools/rules/jarjar_rules", "jarjar_binary")

java_library(
    name = "metricsmgr-lib-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/MetricManager.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/metricsmgr-api/src/java:metricsmgr-api-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/metricsmgr/src/thrift:thrift_scribe_java",
        "//3rdparty/snakeyaml:snakeyaml-java",
        "//3rdparty/fasterxml-jackson:fasterxml-jackson-java",
        "//3rdparty/protobuf:protobuf-java",
        "//3rdparty/thrift:thrift-java",
        "//3rdparty/logging:logging-java", # slf4j is only used for thrift in scribe-sink
    ],
)

java_binary(
    name = "metricsmgr-unshaded",
    srcs = glob(["**/MetricsManager.java"]),
    deps = [
        ":metricsmgr-lib-java",
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/metricsmgr-api/src/java:metricsmgr-api-java",
        "//heron/proto:proto_metrics_java",
    ],
)

jarjar_binary(
    name = "heron-metricsmgr",
    src = ":metricsmgr-unshaded_deploy.jar",
    shade = "shade.conf",
)
