load("/tools/rules/java_tests", "java_tests")

java_library(
    name = "common-tests",
    srcs = glob(["**/*.java"]),
    deps = [
        "//heron/common/src/java:common-java",
        "//heron/proto:proto_networktests_java",
        "//third_party/java:powermock",
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
        "com.twitter.heron.common.network.EchoTest",
        "com.twitter.heron.common.network.HeronServerTest",
        "com.twitter.heron.common.config.ConfigReaderTest",
        "com.twitter.heron.common.config.SystemConfigTest",
    ],
    runtime_deps = [
        ":common-tests",
    ],
    data = glob(["**/config/testdata/**/*.yaml"]),
    size = "small",
)
