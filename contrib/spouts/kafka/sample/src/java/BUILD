load("@rules_java//java:defs.bzl", "java_binary")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

heron_kafka_spout_sample_dep = [
    "//contrib/spouts/kafka/src/java:heron-kafka-spout-java",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "@maven//:org_apache_kafka_kafka_clients",
    "@maven//:org_slf4j_slf4j_api",
]

java_binary(
    name = "heron-kafka-spout-java-sample",
    srcs = glob(["org/apache/heron/spouts/kafka/**/*.java"]),
    deps = heron_kafka_spout_sample_dep,
)
