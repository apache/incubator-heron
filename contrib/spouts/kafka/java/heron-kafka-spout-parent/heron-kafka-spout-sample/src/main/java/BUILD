licenses(["notice"])

package(default_visibility = ["//visibility:public"])

heron_kafka_spout_sample_dep = [
    "//contrib/spouts/kafka/java/heron-kafka-spout-parent/heron-kafka-spout/src/main/java:heron-kafka-spout-java",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/simulator/src/java:simulator-java",
    "@org_apache_kafka_kafka_clients//jar",
    "@org_slf4j_slf4j_api//jar",
]

java_binary(
    name = "heron-kafka-spout-java-sample",
    srcs = glob(["org/apache/heron/spouts/kafka/**/*.java"]),
    deps = heron_kafka_spout_sample_dep,
)