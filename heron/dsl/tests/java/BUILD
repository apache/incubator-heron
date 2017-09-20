load("/tools/rules/heron_deps", "heron_java_api_proto_files")

dsl_core_deps_files = [
    "//heron/dsl/src/java:dsl-unshaded-java",
    "//heron/api/src/java:api-java",
    "//heron/common/src/java:utils-java",
    "//third_party/java:junit4",
    "@org_mockito_mockito_all//jar"
]

dsl_deps_files = \
  heron_java_api_proto_files() + \
  dsl_core_deps_files

java_library(
  name = "dsl-tests",
  srcs = glob(["**/*.java"]),
  deps = dsl_deps_files 
)

java_tests(
  test_classes = [
    "com.twitter.heron.dsl.impl.BaseStreamletTest",
  ],
  runtime_deps = [ ":dsl-tests" ],
  size = "small",
)
