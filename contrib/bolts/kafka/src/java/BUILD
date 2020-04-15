load("@rules_java//java:defs.bzl", "java_library")
load("//tools/rules:build_defs.bzl", "DOCLINT_HTML_AND_SYNTAX")
load("//tools/rules:javadoc.bzl", "java_doc")

licenses(["notice"])

package(default_visibility = ["//visibility:public"])

java_doc(
    name = "heron-kafka-bolt-javadoc",
    libs = [":heron-kafka-bolt-java"],
    pkgs = ["org/apache/heron/bolts/kafka"],
    title = "Kafka Bolt Documentation",
)

kafka_bolt_deps = [
    "//storm-compatibility/src/java:storm-compatibility-java-neverlink",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//third_party/java:logging",
    "@maven//:org_apache_kafka_kafka_clients",
]

java_library(
    name = "heron-kafka-bolt-java",
    srcs = glob(["org/apache/heron/bolts/kafka/**/*.java"]),
    javacopts = DOCLINT_HTML_AND_SYNTAX,
    deps = kafka_bolt_deps,
)
