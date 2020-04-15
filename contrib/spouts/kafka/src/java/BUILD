load("@rules_java//java:defs.bzl", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_doc(
    name = "heron-kafka-spout-javadoc",
    libs = [":heron-kafka-spout-java"],
    pkgs = ["org/apache/heron/spouts/kafka"],
    title = "Kafka Spout Documentation",
)

kafka_spout_deps = [
    "//storm-compatibility/src/java:storm-compatibility-java-neverlink",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//third_party/java:logging",
    "@maven//:org_apache_kafka_kafka_clients",
]

java_library(
    name = "heron-kafka-spout-java",
    srcs = glob(["org/apache/heron/spouts/kafka/**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = kafka_spout_deps,
)
