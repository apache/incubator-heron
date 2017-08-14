load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = "storm-tests",
    srcs = glob(["**/*.java"]),
    deps = heron_java_proto_files() + [
        "//heron/storm/src/java:storm-compatibility-java",
        "//heron/common/src/java:common-java",
        "//heron/api/src/java:api-java",
        "//third_party/java:junit4",
        "//third_party/java:mockito",
    ],
)

java_tests(
    test_classes = [
        "org.apache.storm.windowing.WaterMarkEventGeneratorTest",
        "org.apache.storm.windowing.WindowManagerTest",
        "org.apache.storm.topology.WindowedBoltExecutorTest",
    ],
    runtime_deps = [
        ":storm-tests",
    ],
    size = "small",
)
