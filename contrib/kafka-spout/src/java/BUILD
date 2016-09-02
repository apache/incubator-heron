licenses(["notice"])

package(default_visibility = ["//visibility:public"])

kafka_spout_deps_files = [
    "//heron/api/src/java:api-java",
    "//heron/storm/src/java:storm-compatibility-java",
    "@commons_io_commons_io//jar",
    "@commons_lang_commons_lang//jar",
    "@com_googlecode_json_simple_json_simple//jar",
    "@com_google_guava_guava//jar",
    "@org_apache_curator_curator_framework//jar",
    "@org_apache_curator_curator_client//jar",
    "@org_apache_kafka_kafka_210//jar",
    "@org_apache_kafka_kafka_clients//jar",
    "@org_apache_zookeeper_zookeeper//jar",
    "@org_scala_lang_scala_library//jar",
    "@org_slf4j_slf4j_api//jar",
    "@org_slf4j_slf4j_jdk14//jar",
]

java_library(
    name = "kafka-spout-java",
    srcs = glob(["**/*.java"]),
    deps = kafka_spout_deps_files,
)

java_binary(
    name = "kafka-spout-unshaded",
    srcs = glob(["**/*.java"]),
    deps = kafka_spout_deps_files,
)

genrule(
    name = "heron-kafka-spout",
    srcs = [":kafka-spout-unshaded_deploy.jar"],
    outs = ["heron-kafka-spout.jar"],
    cmd = "cp $< $@",
)