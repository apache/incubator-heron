java_test(
    name = "handle_tmaster_location_unittest",
    srcs = glob(["**/HandleTMasterLocationTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:mockito",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "metrics_manager_server_unittest",
    srcs = glob(["**/MetricsManagerServerTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java", 
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "sink_executor_unittest",
    srcs = glob(["**/SinkExecutorTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto-java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)

java_test(
    name = "tmaster_sink_unittest",
    srcs = glob(["**/TMasterSinkTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_tmaster_java",
        "//3rdparty/protobuf:protobuf-java",
        "@bazel_tools//third_party:junit4",
    ],
    size = "small",
)
