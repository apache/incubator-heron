# Heron Kafka Spout

[Kafka Spout](../heron-kafka-spout/src/main/java/org/apache/heron/spouts/kafka/KafkaSpout.java) enables a Heron topology to consume data from Kafka cluster as input into the stream processing pipeline. Primarily, it is written using 2 APIs, the Heron API and Kafka Client API.

##Configuring the underlying Kafka Consumer

Each Kafka Spout instance creates its underlying [Consumer](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) instance via a factory interface [KafkaConsumerFactory](../heron-kafka-spout/src/main/java/org/apache/heron/spouts/kafka/KafkaConsumerFactory.java) that is passed in as one of the constructor arguments.

The simplest way is to use the provided [DefaultKafkaConsumerFactory](../heron-kafka-spout/src/main/java/org/apache/heron/spouts/kafka/DefaultKafkaConsumerFactory.java). It takes a `Map<String, Object>` as its only input, which should contain all the user configured properties as instituted by [ConsumerConfig](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)

_Note: `enable.auto.commit` is always set to false in `DefaultKafkaConsumerFactory` because the Kafka Spout needs to manually manage the committing of offset. Any custom implementation of `KafkaConsumerFactory` should adhere to the same thing_

```java
Map<String, Object> kafkaConsumerConfig = new HashMap<>();
//connect to Kafka broker at localhost:9092
kafkaConsumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//group ID of the consumer group
kafkaConsumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-kafka-spout");
//key and value serializer
kafkaConsumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
kafkaConsumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

KafkaConsumerFactory<String, String> kafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(kafkaConsumerConfig);
```

##Subscribe to topic(s)

The Kafka Spout instance can be configured to subscribe either a collection of topics by specifying the list of topic name strings in `Collection<String>`, or it can take an implementation of [TopicPatternProvider](../heron-kafka-spout/src/main/java/org/apache/heron/spouts/kafka/TopicPatternProvider.java) to provide a regular expression to match all the topics that it wants to subscribe to. There is a [DefaultTopicPatternProvider](../heron-kafka-spout/src/main/java/org/apache/heron/spouts/kafka/DefaultTopicPatternProvider.java) to convert a regex string to a pattern.

```java
//subscribe to specific named topic
new KafkaSpout<>(kafkaConsumerFactory, Collections.singletonList("test-topic"))

//subscribe to topics matching a pattern
new KafkaSpout<>(kafkaConsumerFactory, new DefaultTopicPatternProvider("test-.*"));
```