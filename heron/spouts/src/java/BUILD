package(default_visibility = ["//visibility:public"])

api_deps_files = [
  "//heron/api/src/java:api-java",
  "//heron/common/src/java:basics-java",
  "//heron/storm/src/java:storm-compatibility-java",
]

pulsar_deps_files = \
  api_deps_files + [
    "//third_party/java:yahoo-pulsar-client",
    "//third_party/java:logging"
  ]

java_library(
  name = 'pulsar-connectors-java',
  srcs = glob(["**/pulsar/**/*.java"]),
  deps = pulsar_deps_files,  
)

java_binary(
    name = 'pulsar-connectors-unshaded',
    srcs = glob(["**/pulsar/**/*.java"]),
    deps = pulsar_deps_files)

genrule(
    name = "heron-pulsar-connectors",
    srcs = [":pulsar-connectors-unshaded_deploy.jar"],
    outs = ["heron-pulsar-connectors.jar"],
    cmd  = "cp $< $@",
)
