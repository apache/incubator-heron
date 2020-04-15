load("@rules_java//java:defs.bzl", "java_test")

heron_kafka_bolts_test_dep = [
    "//contrib/bolts/kafka/src/java:heron-kafka-bolt-java",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//third_party/java:junit4",
    "@maven//:org_apache_kafka_kafka_clients",
    "@maven//:org_mockito_mockito_all",
]

java_test(
    name = "KafkaBoltTest",
    srcs = ["org/apache/heron/bolts/kafka/KafkaBoltTest.java"],
    test_class = "org.apache.heron.bolts.kafka.KafkaBoltTest",
    deps = heron_kafka_bolts_test_dep,
)

java_test(
    name = "DefaultKafkaProducerFactoryTest",
    srcs = ["org/apache/heron/bolts/kafka/DefaultKafkaProducerFactoryTest.java"],
    test_class = "org.apache.heron.bolts.kafka.DefaultKafkaProducerFactoryTest",
    deps = heron_kafka_bolts_test_dep,
)
