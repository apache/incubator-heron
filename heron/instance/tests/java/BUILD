load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

test_deps_files = \
    heron_java_proto_files() + [
        "//heron/api/src/java:api-java",
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
    test_classes = [
        "com.twitter.heron.grouping.CustomGroupingTest",
        "com.twitter.heron.grouping.EmitDirectBoltTest",
        "com.twitter.heron.grouping.EmitDirectSpoutTest",
        "com.twitter.heron.instance.bolt.BoltInstanceTest",
        "com.twitter.heron.instance.spout.ActivateDeactivateTest",
        "com.twitter.heron.instance.spout.SpoutInstanceTest",
        "com.twitter.heron.metrics.GlobalMetricsTest",
        "com.twitter.heron.metrics.MultiAssignableMetricTest",
        "com.twitter.heron.network.ConnectTest",
        "com.twitter.heron.network.HandleReadTest",
        "com.twitter.heron.network.HandleWriteTest",
    ],
    runtime_deps = [
        ":instance-tests",
    ],
    data = ["//heron/config/src/yaml:test-config-internals-yaml"],
    size = "small",
)
