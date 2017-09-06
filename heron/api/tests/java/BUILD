load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_api_proto_files")

api_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:utils-java",
    "//third_party/java:junit4",
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
    "com.twitter.heron.api.bolt.BaseWindowedBoltTest"
  ],
  runtime_deps = [ ":api-tests" ],
  size = "small",
)
