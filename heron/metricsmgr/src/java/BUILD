licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_library(
    name = "metricsmgr-java",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/MetricManager.java"],
    ),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/metricsmgr/src/thrift:thrift_scribe_java",
        "//3rdparty/java:jackson",
        "@snakeyaml//jar",
        "@protobuf_java//jar",
        "@thrift//jar",
        "//3rdparty/java:logging", # slf4j is only used for thrift in scribe-sink
    ],
)

java_binary(
    name = "metricsmgr-unshaded",
    srcs = glob(["**/MetricsManager.java"]),
    deps = [
        ":metricsmgr-java",
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//heron/proto:proto_metrics_java",
    ],
)

genrule(
    name = "heron-metricsmgr",
    srcs = [":metricsmgr-unshaded_deploy.jar"],
    outs = ["heron-metricsmgr.jar"],
    cmd  = "cp $< $@",
)
