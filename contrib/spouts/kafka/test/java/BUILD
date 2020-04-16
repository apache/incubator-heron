load("@rules_java//java:defs.bzl", "java_test")

heron_kafka_spouts_test_dep = [
    "//contrib/spouts/kafka/src/java:heron-kafka-spout-java",
    "//heron/api/src/java:api-java-low-level",
    "//heron/common/src/java:basics-java",
    "//heron/common/src/java:config-java",
    "//third_party/java:junit4",
    "@maven//:org_apache_kafka_kafka_clients",
    "@maven//:org_mockito_mockito_all",
]

java_test(
    name = "KafkaSpoutTest",
    srcs = ["org/apache/heron/spouts/kafka/KafkaSpoutTest.java"],
    test_class = "org.apache.heron.spouts.kafka.KafkaSpoutTest",
    deps = heron_kafka_spouts_test_dep,
)

java_test(
    name = "KafkaMetricDecoratorTest",
    srcs = ["org/apache/heron/spouts/kafka/KafkaMetricDecoratorTest.java"],
    test_class = "org.apache.heron.spouts.kafka.KafkaMetricDecoratorTest",
    deps = heron_kafka_spouts_test_dep,
)

java_test(
    name = "DefaultTopicPatternProviderTest",
    srcs = ["org/apache/heron/spouts/kafka/DefaultTopicPatternProviderTest.java"],
    test_class = "org.apache.heron.spouts.kafka.DefaultTopicPatternProviderTest",
    deps = heron_kafka_spouts_test_dep,
)

java_test(
    name = "DefaultKafkaConsumerFactoryTest",
    srcs = ["org/apache/heron/spouts/kafka/DefaultKafkaConsumerFactoryTest.java"],
    test_class = "org.apache.heron.spouts.kafka.DefaultKafkaConsumerFactoryTest",
    deps = heron_kafka_spouts_test_dep,
)

java_test(
    name = "DefaultConsumerRecordTransformerTest",
    srcs = ["org/apache/heron/spouts/kafka/DefaultConsumerRecordTransformerTest.java"],
    test_class = "org.apache.heron.spouts.kafka.DefaultConsumerRecordTransformerTest",
    deps = heron_kafka_spouts_test_dep,
)
