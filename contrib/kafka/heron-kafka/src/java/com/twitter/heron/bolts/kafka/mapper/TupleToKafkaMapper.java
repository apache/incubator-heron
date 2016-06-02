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

import java.io.Serializable;

import com.twitter.heron.api.tuple.Tuple;

/**
 * as the really verbose name suggests this interface mapps a storm tuple to kafka key and message.
 * @param <K> type of key.
 * @param <V> type of value.
 */
public interface TupleToKafkaMapper<K, V> extends Serializable {
  K getKeyFromTuple(Tuple tuple);

  V getMessageFromTuple(Tuple tuple);
}
