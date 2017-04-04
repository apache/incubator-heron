load("/tools/rules/java_tests", "java_tests")

java_library(
    name = "metricsmgr-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/metricsmgr/src/java:metricsmgr-java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "com.twitter.heron.metricsmgr.HandleTMasterLocationTest",
        "com.twitter.heron.metricsmgr.MetricsManagerServerTest",
        "com.twitter.heron.metricsmgr.executor.SinkExecutorTest",
        "com.twitter.heron.metricsmgr.sink.tmaster.TMasterSinkTest",
        "com.twitter.heron.metricsmgr.sink.FileSinkTest",
        "com.twitter.heron.metricsmgr.sink.WebSinkTest",
    ],
    runtime_deps = [
        ":metricsmgr-tests",
    ],
    size = "small",
)
