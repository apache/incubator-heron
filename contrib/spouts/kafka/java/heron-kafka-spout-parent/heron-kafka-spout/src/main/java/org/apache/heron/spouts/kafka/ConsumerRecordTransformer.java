/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.heron.spouts.kafka;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * This is the transformer class whose responsibility is to:
 *
 * <ol>
 * <li>define the id of the output streams</li>
 * <li>declare the list of fields of the output tuple</li>
 * <li>translate the incoming Kafka record into the list of values of the output tuple</li>
 * </ol>
 * <p>
 * The default behavior of the built-in transformer will output to stream "default",
 * with 2 fields, "key" and "value" which are the key and value field of the incoming Kafka record.
 *
 * @param <K> the type of the key of the Kafka record
 * @param <V> the type of the value of the Kafka record
 * @see KafkaSpout#setConsumerRecordTransformer(ConsumerRecordTransformer)
 */
public interface ConsumerRecordTransformer<K, V> extends Serializable {
  default List<String> getOutputStreams() {
    return Collections.singletonList("default");
  }

  default List<String> getFieldNames(String streamId) {
    return Arrays.asList("key", "value");
  }

  default Map<String, List<Object>> transform(ConsumerRecord<K, V> record) {
    return Collections.singletonMap("default", Arrays.asList(record.key(), record.value()));
  }
}
