api_deps_files = [
    "//heron/api/src/java:api-java-low-level",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:utils-java",
    "//heron/common/src/java:basics-java",
    "//third_party/java:junit4",
    "//third_party/java:powermock",
    "//third_party/java:kryo",
    "@org_mockito_mockito_all//jar"
]

api_deps_files = \
  heron_java_api_proto_files() + \
  api_deps_files

java_library(
  name = "api-tests",
  srcs = glob(["**/*.java"]),
  deps = api_deps_files
)

java_tests(
  test_classes = [
    "org.apache.heron.api.windowing.WindowManagerTest",
    "org.apache.heron.api.windowing.WaterMarkEventGeneratorTest",
    "org.apache.heron.api.bolt.WindowedBoltExecutorTest",
    "org.apache.heron.api.metric.CountStatAndMetricTest",
    "org.apache.heron.api.metric.LatencyStatAndMetricTest",
    "org.apache.heron.api.bolt.BaseWindowedBoltTest",
    "org.apache.heron.streamlet.impl.StreamletImplTest",
    "org.apache.heron.streamlet.impl.operators.JoinOperatorTest",
    "org.apache.heron.streamlet.impl.operators.ReduceByKeyAndWindowOperatorTest",
    "org.apache.heron.streamlet.impl.operators.GeneralReduceByKeyAndWindowOperatorTest",
    "org.apache.heron.api.ConfigTest",
    "org.apache.heron.api.HeronSubmitterTest"
  ],
  runtime_deps = [ ":api-tests" ],
  size = "small",
)
