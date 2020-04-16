load("@rules_java//java:defs.bzl", "java_library")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java-low-level",
        "//heron/common/src/java:basics-java",
        "//heron/common/src/java:config-java",
        "//heron/common/src/java:network-java",
        "//heron/common/src/java:utils-java",
        "//heron/common/src/java:test-helpers-java",
        "//heron/instance/src/java:instance-java",
        "//third_party/java:junit4",
        "//third_party/java:kryo",
        "//third_party/java:mockito",
    ]

java_library(
    name = "instance-tests",
    srcs = glob(["**/*.java"]),
    deps = test_deps_files,
)

java_tests(
    size = "small",
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    test_classes = [
        "org.apache.heron.grouping.CustomGroupingTest",
        "org.apache.heron.grouping.EmitDirectBoltTest",
        "org.apache.heron.grouping.EmitDirectSpoutTest",
        "org.apache.heron.instance.bolt.BoltInstanceTest",
        "org.apache.heron.instance.bolt.BoltStatefulInstanceTest",
        "org.apache.heron.instance.spout.ActivateDeactivateTest",
        "org.apache.heron.instance.spout.SpoutInstanceTest",
        "org.apache.heron.instance.spout.SpoutStatefulInstanceTest",
        "org.apache.heron.metrics.GlobalMetricsTest",
        "org.apache.heron.metrics.MultiAssignableMetricTest",
        "org.apache.heron.network.ConnectTest",
        "org.apache.heron.network.HandleReadTest",
        "org.apache.heron.network.HandleWriteTest",
    ],
    runtime_deps = [
        ":instance-tests",
    ],
)
