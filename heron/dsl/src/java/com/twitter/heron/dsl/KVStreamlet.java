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

package com.twitter.heron.dsl;

import java.util.function.BiFunction;

import com.twitter.heron.dsl.windowing.WindowConfig;

/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
 csv files, HDFS files), or for that matter any other source. They are also created by
 transforming existing Streamlets using operations such as map/flatMap, etc.
 Besides the tuples, a Streamlet has the following properties associated with it
 a) name. User assigned or system generated name to refer the streamlet
 b) nPartitions. Number of partitions that the streamlet is composed of. The nPartitions
 could be assigned by the user or computed by the system
 */
public abstract class KVStreamlet<K, V> extends Streamlet<KeyValue<K, V>> {
  <V2, VR> KVStreamlet<K, VR> join(WindowConfig windowCfg,
                                   KVStreamlet<K, V2> other,
                                   BiFunction<V, V2, VR> joinFunction) {
    return new JoinStreamlet<K, V, V2, VR>(this, other, windowCfg, joinFunction);
  }

  /*
  KVStreamlet<K, V> reduceByKeyAndWindow(com.twitter.heron.dsl.WindowConfig windowCfg,
                                         BinaryOperator<V> reduceFn) {

  }
  */
}
