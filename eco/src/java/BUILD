licenses(["notice"])

package(default_visibility = ["//visibility:public"])

eco_deps = [
    "@commons_cli_commons_cli//jar",
    "@org_yaml_snakeyaml//jar",
    "//third_party/java:logging",
    "//storm-compatibility/src/java:storm-compatibility-java-neverlink",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
]

java_library(
    name = "eco-java",
    srcs = glob(["org/apache/heron/eco/**/*.java"]),
    deps = eco_deps
)
