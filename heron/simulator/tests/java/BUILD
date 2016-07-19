load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

java_library(
    name = "simulator-tests",
    srcs = glob(["**/*.java"]),
    deps =  heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
        "//heron/common/src/java:common-java",
        "//heron/simulator/src/java:simulator-java",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
)

java_tests(
    test_classes = [
        "com.twitter.heron.simulator.executors.InstanceExecutorTest",
        "com.twitter.heron.simulator.grouping.AllGroupingTest",
        "com.twitter.heron.simulator.grouping.CustomGroupingTest",
        "com.twitter.heron.simulator.grouping.FieldsGroupingTest",
        "com.twitter.heron.simulator.grouping.LowestGroupingTest",
        "com.twitter.heron.simulator.grouping.ShuffleGroupingTest",
        "com.twitter.heron.simulator.utils.PhysicalPlanUtilTest",
        "com.twitter.heron.simulator.utils.RotatingMapTest",
        "com.twitter.heron.simulator.utils.StreamConsumersTest",
        "com.twitter.heron.simulator.utils.TupleCacheTest",
        "com.twitter.heron.simulator.utils.XORManagerTest",
        "com.twitter.heron.simulator.SimulatorTest",
    ],
    runtime_deps = [
        ":simulator-tests",
    ],
    size = "small",
)
