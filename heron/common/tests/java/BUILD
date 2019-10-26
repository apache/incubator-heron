java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/proto:proto_topology_java",
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/common/src/java:test-helpers-java",
        "//heron/proto:proto_networktests_java",
        "//third_party/java:powermock",
        "@commons_io_commons_io//jar",
        "@com_google_protobuf//:protobuf_java",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "org.apache.heron.common.basics.ByteAmountTest",
        "org.apache.heron.common.basics.CommunicatorTest",
        "org.apache.heron.common.basics.SysUtilsTest",
        "org.apache.heron.common.basics.WakeableLooperTest",
        "org.apache.heron.common.basics.FileUtilsTest",
        "org.apache.heron.common.basics.PackageTypeTest",
        "org.apache.heron.common.test.EchoTest",
        "org.apache.heron.common.test.HeronServerTest",
        "org.apache.heron.common.config.ConfigReaderTest",
        "org.apache.heron.common.config.SystemConfigTest",
        "org.apache.heron.common.utils.TopologyUtilsTest",
    ],
    runtime_deps = [
        ":common-tests",
    ],
    resources = ["//heron/common/tests/resources:common-resources"],
    size = "small",
)
