load("@rules_java//java:defs.bzl", "java_library")

java_library(
    name = "metricsmgr-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:test-helpers-java",
        "//heron/metricsmgr/src/java:metricsmgr-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
        "@com_google_protobuf//:protobuf_java",
        "@maven//:com_google_guava_guava",
    ],
)

java_tests(
    size = "small",
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    test_classes = [
        "org.apache.heron.metricsmgr.MetricsUtilTests",
        "org.apache.heron.metricsmgr.HandleTMasterLocationTest",
        "org.apache.heron.metricsmgr.MetricsManagerServerTest",
        "org.apache.heron.metricsmgr.executor.SinkExecutorTest",
        "org.apache.heron.metricsmgr.sink.tmaster.TMasterSinkTest",
        "org.apache.heron.metricsmgr.sink.metricscache.MetricsCacheSinkTest",
        "org.apache.heron.metricsmgr.sink.FileSinkTest",
        "org.apache.heron.metricsmgr.sink.WebSinkTest",
        "org.apache.heron.metricsmgr.sink.PrometheusSinkTests",
    ],
    runtime_deps = [
        ":metricsmgr-tests",
    ],
)
