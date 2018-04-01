licenses(["notice"])

package(default_visibility = ["//visibility:public"])

x86_files = glob(
    ["**/*.java"],
    exclude = ["**/MetricManager.java"],
)

non_x86_files = glob(
    ["**/*.java"],
    exclude = ["**/MetricManager.java", "**/sink/scribe/ScribeSink.java"],
)

non_x86_deps = [
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//heron/common/src/java:network-java",
    "//heron/common/src/java:utils-java",
    "//heron/spi/src/java:metricsmgr-spi-java",
    "//heron/proto:proto_common_java",
    "//heron/proto:proto_metrics_java",
    "//heron/proto:proto_tmaster_java",
    "//third_party/java:guava", # only used in WebSink
    "//third_party/java:jackson",
    "//third_party/java:cli",
    "//third_party/java:logging", # slf4j is only used for thrift in scribe-sink
    "@com_google_protobuf//:protobuf_java",
    "@org_apache_thrift_libthrift//jar",
    "@org_yaml_snakeyaml//jar",
]

x86_deps = non_x86_deps + [
    "//heron/metricsmgr/src/thrift:thrift_scribe_java",
]

java_library(
    name = "metricsmgr-java",
    srcs = select({
        "//tools/platform:darwin": x86_files,
        "//tools/platform:k8": x86_files,
        "//conditions:default" : non_x86_files,
    }),
    deps = select({
        "//tools/platform:darwin": x86_deps,
        "//tools/platform:k8": x86_deps,
        "//conditions:default" : non_x86_deps,
    }),
)

java_binary(
    name = "metricsmgr-unshaded",
    srcs = glob(["**/MetricsManager.java"]),
    deps = [
        ":metricsmgr-java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//heron/proto:proto_metrics_java",
        "//third_party/java:cli",
    ],
)

genrule(
    name = "heron-metricsmgr",
    srcs = [":metricsmgr-unshaded_deploy.jar"],
    outs = ["heron-metricsmgr.jar"],
    cmd  = "cp $< $@",
)
