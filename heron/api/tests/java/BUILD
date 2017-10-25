load("/tools/rules/heron_deps", "heron_java_api_proto_files")

api_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:utils-java",
    "//heron/common/src/java:basics-java",
    "//third_party/java:junit4",
    "//third_party/java:powermock",
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
    "com.twitter.heron.api.windowing.WindowManagerTest",
    "com.twitter.heron.api.windowing.WaterMarkEventGeneratorTest",
    "com.twitter.heron.api.bolt.WindowedBoltExecutorTest",
    "com.twitter.heron.api.metric.CountStatAndMetricTest",
    "com.twitter.heron.api.metric.LatencyStatAndMetricTest",
    "com.twitter.heron.api.bolt.BaseWindowedBoltTest",
    "com.twitter.heron.streamlet.impl.BaseStreamletTest",
    "com.twitter.heron.streamlet.impl.BaseKVStreamletTest",
    "com.twitter.heron.streamlet.impl.operators.JoinOperatorTest",
    "com.twitter.heron.api.HeronSubmitterTest"
  ],
  runtime_deps = [ ":api-tests" ],
  size = "small",
)
