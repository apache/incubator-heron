/*
 * Copyright 2019
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Serializable;

/**
 * the factory to create the underlying KafkaConsumer instance the Kafka Spout will be using to consume data from Kafka cluster
 *
 * @param <K> the type of the key of the Kafka record
 * @param <V> the type of the value of the Kafka record
 */
public interface KafkaConsumerFactory<K, V> extends Serializable {
    /**
     * create the underlying KafkaConsumer
     *
     * @return kafka consumer instance
     */
    Consumer<K, V> create();
}
