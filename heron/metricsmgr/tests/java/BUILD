java_test(
    name = "HandleTMasterLocationTest",
    srcs = glob(["**/HandleTMasterLocationTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java",
        "//heron/proto:proto_tmaster_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "MetricsManagerServerTest",
    srcs = glob(["**/MetricsManagerServerTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_common_java",
        "//heron/proto:proto_metrics_java", 
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "SinkExecutorTest",
    srcs = glob(["**/SinkExecutorTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto-java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "TMasterSinkTest",
    srcs = glob(["**/TMasterSinkTest.java"]),
    deps = [
        "//heron/api/src/java:api-java", 
        "//heron/common/src/java:common-java", 
        "//heron/spi/src/java:metricsmgr-spi-java", 
        "//heron/metricsmgr/src/java:metricsmgr-java", 
        "//heron/proto:proto_tmaster_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
    ],
    size = "small",
)

java_test(
    name = "FileSinkTest",
    srcs = glob(["**/FileSinkTest.java"]),
    deps = [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/spi/src/java:metricsmgr-spi-java",
        "//heron/metricsmgr/src/java:metricsmgr-java",
        "//heron/proto:proto_tmaster_java",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
    ],
    size = "small",
)
