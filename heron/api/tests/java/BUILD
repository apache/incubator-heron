load("@rules_java//java:defs.bzl", "java_library")

local_api_deps_files = [
    "//heron/api/src/java:api-java-low-level",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:utils-java",
    "//heron/common/src/java:basics-java",
    "//third_party/java:junit4",
    "//third_party/java:powermock",
    "//third_party/java:kryo",
    "@maven//:org_mockito_mockito_all",
]

api_deps_files = \
    heron_java_api_proto_files() + \
    local_api_deps_files

java_library(
    name = "api-tests",
    srcs = glob(["**/*.java"]),
    deps = api_deps_files,
)

java_tests(
    size = "small",
    test_classes = [
        "org.apache.heron.api.windowing.WindowManagerTest",
        "org.apache.heron.api.windowing.WaterMarkEventGeneratorTest",
        "org.apache.heron.api.bolt.WindowedBoltExecutorTest",
        "org.apache.heron.api.metric.CountStatAndMetricTest",
        "org.apache.heron.api.metric.LatencyStatAndMetricTest",
        "org.apache.heron.api.bolt.BaseWindowedBoltTest",
        "org.apache.heron.streamlet.impl.StreamletImplTest",
        "org.apache.heron.streamlet.impl.operators.JoinOperatorTest",
        "org.apache.heron.streamlet.impl.operators.KeyByOperatorTest",
        "org.apache.heron.streamlet.impl.operators.ReduceByKeyAndWindowOperatorTest",
        "org.apache.heron.streamlet.impl.operators.GeneralReduceByKeyAndWindowOperatorTest",
        "org.apache.heron.streamlet.impl.streamlets.KVStreamletShadowTest",
        "org.apache.heron.streamlet.impl.streamlets.StreamletShadowTest",
        "org.apache.heron.streamlet.impl.utils.StreamletUtilsTest",
        "org.apache.heron.api.ConfigTest",
        "org.apache.heron.api.HeronSubmitterTest",
        "org.apache.heron.api.utils.UtilsTest",
    ],
    runtime_deps = [":api-tests"],
)
