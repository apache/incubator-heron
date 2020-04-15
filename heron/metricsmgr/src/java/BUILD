load("@rules_java//java:defs.bzl", "java_binary", "java_library")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

files = glob(
    ["**/*.java"],
    exclude = ["**/MetricManager.java"],
)

deps = [
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/common/src/java:network-java",
    "//heron/common/src/java:utils-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_tmaster_java",
    "//third_party/java:guava",  # only used in WebSink
    "//third_party/java:jackson",
    "//third_party/java:cli",
    "@com_google_protobuf//:protobuf_java",
    "@maven//:org_yaml_snakeyaml",
]

filegroup(
    name = "metricsmgr-main",
    srcs = glob(["**/MetricsManager.java"]),
)

java_library(
    name = "metricsmgr-java",
    srcs = files,
    deps = deps,
)

java_binary(
    name = "metricsmgr-unshaded",
    srcs = [":metricsmgr-main"],
    deps = [
        ":metricsmgr-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/proto:proto_metrics_java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//third_party/java:cli",
    ],
)

genrule(
    name = "heron-metricsmgr",
    srcs = [":metricsmgr-unshaded_deploy.jar"],
    outs = ["heron-metricsmgr.jar"],
    cmd = "cp $< $@",
)
