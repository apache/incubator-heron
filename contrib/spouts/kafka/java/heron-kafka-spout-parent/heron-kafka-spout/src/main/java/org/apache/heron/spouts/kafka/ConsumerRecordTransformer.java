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

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * This is the transformer class whose responsibility is to:
 *
 * <ol>
 * <li>define the id of the output stream</li>
 * <li>declare the list of fields of the output tuple</li>
 * <li>translate the incoming Kafka record into the list of values of the output tuple</li>
 * </ol>
 * <p>
 * The default behavior of the built-in transformer will output to stream "default", with 2 fields, "key" and "value" which are the key and value field of the incoming Kafka record. It's chosen that one Kafka record results in one tuple, which makes it straight forward to the acknowledge the spout tuple when acknowledgement is turned on in the topology. Multiple spouts can consume the same topic (with different group id) if the same record needs to yield multiple input tuples into a topology.
 *
 * @param <K> the type of the key of the Kafka record
 * @param <V> the type of the value of the Kafka record
 * @see KafkaSpout#setConsumerRecordTransformer(ConsumerRecordTransformer)
 */
public interface ConsumerRecordTransformer<K, V> extends Serializable {
    default String getOutputStream() {
        return "default";
    }

    default List<String> getFieldNames() {
        return Arrays.asList("key", "value");
    }

    default List<Object> transform(ConsumerRecord<K, V> record) {
        return Arrays.asList(record.key(), record.value());
    }
}
