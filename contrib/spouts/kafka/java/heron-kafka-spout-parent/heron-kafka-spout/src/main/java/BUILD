licenses(["notice"])

package(default_visibility = ["//visibility:public"])

kafka_spout_deps = [
    "//storm-compatibility/src/java:storm-compatibility-java-neverlink",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//third_party/java:logging",
    "@org_apache_kafka_kafka_clients//jar",
]

java_library(
    name = "heron-kafka-spout-java",
    srcs = glob(["org/apache/heron/spouts/kafka/**/*.java"]),
    deps = kafka_spout_deps,
)
