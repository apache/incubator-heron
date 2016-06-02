// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.bolts.kafka.mapper;

import java.util.Map;

import com.twitter.heron.api.tuple.Tuple;

/**
 * Maps key and message from tuples produced by KeyValueScheme deserializer in Kafka spout.
 */
@SuppressWarnings({"unchecked", "serial"})
public class KafkaMirrorMapper<K, V> implements TupleToKafkaMapper<K, V> {

  private final String kafkaMsgField;

  private static final String DEFAULT_KAFKA_MESSAGE_FIELD = "bytes";

  public KafkaMirrorMapper() {
    this(DEFAULT_KAFKA_MESSAGE_FIELD);
  }

  public KafkaMirrorMapper(final String kafkaMsgField) {
    this.kafkaMsgField = kafkaMsgField;
  }

  @Override
  public K getKeyFromTuple(Tuple tuple) {
    Object kafkaMsg = tuple.contains(kafkaMsgField) ? tuple.getValueByField(kafkaMsgField) : null;
    if (kafkaMsg != null && kafkaMsg instanceof Map) {
      Map<K, V> kvMap = (Map<K, V>) kafkaMsg;
      if (kvMap.size() > 0) {
        return kvMap.keySet().iterator().next();
      }
    }

    return null;
  }

  @Override
  public V getMessageFromTuple(Tuple tuple) {
    Object kafkaMsg = tuple.contains(kafkaMsgField) ? tuple.getValueByField(kafkaMsgField) : null;
    if (kafkaMsg != null) {
      if (kafkaMsg instanceof Map) {
        Map<K, V> kvMap = (Map<K, V>) kafkaMsg;
        if (kvMap.size() > 0) {
          return kvMap.values().iterator().next();
        }
      } else {
        return (V) kafkaMsg;
      }
    }

    return null;
  }
}
