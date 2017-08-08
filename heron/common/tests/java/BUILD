load("/tools/rules/java_tests", "java_tests")

java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:test-helpers-java",
        "//heron/proto:proto_networktests_java",
        "//third_party/java:powermock",
        "@commons_io_commons_io//jar",
        "@com_google_protobuf_protobuf_java//jar",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "com.twitter.heron.common.basics.ByteAmountTest",
        "com.twitter.heron.common.basics.CommunicatorTest",
        "com.twitter.heron.common.basics.SysUtilsTest",
        "com.twitter.heron.common.basics.WakeableLooperTest",
        "com.twitter.heron.common.basics.FileUtilsTest",
        "com.twitter.heron.common.basics.PackageTypeTest",
        "com.twitter.heron.common.test.EchoTest",
        "com.twitter.heron.common.test.HeronServerTest",
        "com.twitter.heron.common.config.ConfigReaderTest",
        "com.twitter.heron.common.config.SystemConfigTest",
    ],
    runtime_deps = [
        ":common-tests",
    ],
    resources = ["//heron/common/tests/resources:common-resources"],
    size = "small",
)
