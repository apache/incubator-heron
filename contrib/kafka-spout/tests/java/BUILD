package(default_visibility = ["//visibility:public"])

load("/tools/rules/java_tests", "java_tests")

java_library(
    name = "kafka-spout-tests",
    srcs = glob(["**/*.java"]),
    deps = [
      "//contrib/kafka-spout/src/java:kafka-spout-java",
      "//heron/storm/src/java:storm-compatibility-java",
      "//third_party/java:junit4",
      "@commons_io_commons_io//jar",
      "@com_google_guava_guava//jar",
      "@org_apache_curator_curator_client//jar",
      "@org_apache_curator_curator_framework//jar",
      "@org_apache_curator_curator_test//jar",
      "@org_apache_kafka_kafka_210//jar",
      "@org_apache_kafka_kafka_clients//jar",
      "@org_apache_zookeeper_zookeeper//jar",
      "@org_mockito_mockito_all//jar",
      "@org_slf4j_slf4j_api//jar",
      "@log4j_log4j//jar",
      "@com_yammer_metrics_metrics_core//jar",
      "@org_javassist_javassist//jar",
      "@com_101tec_zkclient//jar",
    ],
)

java_tests(
    test_classes = [
        "org.apache.storm.kafka.DynamicBrokersReaderTest",
        "org.apache.storm.kafka.ExponentialBackoffMsgRetryManagerTest",
        "org.apache.storm.kafka.KafkaErrorTest",
        "org.apache.storm.kafka.KafkaUtilsTest",
        "org.apache.storm.kafka.StringKeyValueSchemeTest",
        "org.apache.storm.kafka.TestStringScheme",
        "org.apache.storm.kafka.ZkCoordinatorTest",
    ],
    runtime_deps = [
        ":kafka-spout-tests",
    ],
    size = "small",
)