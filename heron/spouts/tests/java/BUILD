load("/tools/rules/java_tests", "java_tests")
load("/tools/rules/heron_deps", "heron_java_proto_files")

common_deps_files = [
    "//third_party/java:powermock",
    "@commons_io_commons_io//jar",
    "//third_party/java:mockito",
    "//third_party/java:junit4",
]

api_deps_files = [
  "//heron/api/src/java:api-java",
  "//heron/common/src/java:basics-java",
  "//heron/storm/src/java:storm-compatibility-java",
]

java_test(
    name = "PulsarSpoutTests",
    srcs = glob(["**/pulsar/**/*.java"]),
    deps = common_deps_files + api_deps_files + [
      "//heron/spouts/src/java:heron-pulsar-connectors",
      "//third_party/java:yahoo-pulsar-client",
    ],  
)
