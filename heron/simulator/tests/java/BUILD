load("@rules_java//java:defs.bzl", "java_library")

java_library(
    name = "simulator-tests",
    srcs = glob(["**/*.java"]),
    deps = heron_java_proto_files() + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:utils-java",
        "//heron/instance/src/java:instance-java",
        "//heron/simulator/src/java:simulator-java",
        "//third_party/java:mockito",
        "//third_party/java:junit4",
    ],
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.simulator.executors.InstanceExecutorTest",
        "org.apache.heron.simulator.grouping.AllGroupingTest",
        "org.apache.heron.simulator.grouping.CustomGroupingTest",
        "org.apache.heron.simulator.grouping.FieldsGroupingTest",
        "org.apache.heron.simulator.grouping.LowestGroupingTest",
        "org.apache.heron.simulator.grouping.ShuffleGroupingTest",
        "org.apache.heron.simulator.utils.TopologyManagerTest",
        "org.apache.heron.simulator.utils.RotatingMapTest",
        "org.apache.heron.simulator.utils.TupleCacheTest",
        "org.apache.heron.simulator.utils.XORManagerTest",
        "org.apache.heron.simulator.SimulatorTest",
    ],
    runtime_deps = [
        ":simulator-tests",
    ],
)
