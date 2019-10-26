<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->
# Heron Kafka Spout

[Kafka Spout](../src/java/org/apache/heron/spouts/kafka/KafkaSpout.java) enables a Heron topology to consume data from Kafka cluster as input into the stream processing pipeline. Primarily, it is written using 2 APIs, the Heron API and Kafka Client API.

##Configuring the underlying Kafka Consumer

Each Kafka Spout instance creates its underlying [Consumer](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) instance via a factory interface [KafkaConsumerFactory](../src/java/org/apache/heron/spouts/kafka/KafkaConsumerFactory.java) that is passed in as one of the constructor arguments.

The simplest way is to use the provided [DefaultKafkaConsumerFactory](../src/java/org/apache/heron/spouts/kafka/DefaultKafkaConsumerFactory.java). It takes a `Map<String, Object>` as its only input, which should contain all the user configured properties as instituted by [ConsumerConfig](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html)

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

The Kafka Spout instance can be configured to subscribe either a collection of topics by specifying the list of topic name strings in `Collection<String>`, or it can take an implementation of [TopicPatternProvider](../src/java/org/apache/heron/spouts/kafka/TopicPatternProvider.java) to provide a regular expression to match all the topics that it wants to subscribe to. There is a [DefaultTopicPatternProvider](../src/java/org/apache/heron/spouts/kafka/DefaultTopicPatternProvider.java) to convert a regex string to a pattern.

```java
//subscribe to specific named topic
new KafkaSpout<>(kafkaConsumerFactory, Collections.singletonList("test-topic"))

//subscribe to topics matching a pattern
new KafkaSpout<>(kafkaConsumerFactory, new DefaultTopicPatternProvider("test-.*"));
```

##Convert ConsumerRecord to Tuple

The Spout delegates the conversion of each Kafka [ConsumerRecord](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) into an output tuple to the [ConsumerRecordTransformer](../src/java/org/apache/heron/spouts/kafka/ConsumerRecordTransformer.java), the [DefaultConsumerRecordTransformer](../src/java/org/apache/heron/spouts/kafka/DefaultConsumerRecordTransformer.java) is provided to simply convert the incoming record into a tuple with 2 fields: "key", being the key of the record, and "value", being the value of the record, and also defines the output stream to be the "default" stream.

User can create their own implementation of the `ConsumerRecordTransformer` interface, and set it to the `KafkaSpout` via [setConsumerRecordTransformer](../src/java/org/apache/heron/spouts/kafka/KafkaSpout.java) method.

##Behavior in Different Topology Reliability Mode

### `ATMOST_ONCE` mode
The whole topology will not turn the `acking` mechanism on. so, the KafkaSpout can afford to emit the tuple without any message id, and it also immediately commit the currently-read offset back to Kafka broker, and neither `ack()` nor `fail()` callback will be invoked. Therefore, "in-flight" tuple will just get lost in case the KafkaSpout instance is blown up or the topology is restarted. That's what `ATMOST_ONCE` offers.

### `ATLEAST_ONCE` mode
The `acking` mechanism is turned on topology-wise, so the KafkaSpout uses the `ack registry` to keep tracking all the **continuous** acknowledgement ranges for each partition, while the `failure registry` keeps tracking the **lowest** failed acknowledgement for each partition. When it comes to the time that the Kafka Consumer needs to poll the Kafka cluster for more records (because it's emitted everything it got from the previous poll), then the KafkaSpout reconciles as following for each partition that it is consuming:

1. if there's any failed tuple, seek back to the lowest corresponding offset
2. discard all the acknowledgements that it's received but is greater than the lowest failed offset
3. clear the lowest failed offset in `failure registry`
4. commit the offset to be the upper boundary of the first range in the `ack registry`

then, it polls the Kafka cluster for next batch of records (i.e. from the lowest failed tuple if any)

So, it guarantees each tuple emitted by the KafkaSpout must be successfully processed across the whole topology at least once.

### `EFFECTIVE_ONCE`
Not implemented yet