/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.streamlet.impl.operators;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableFunction;

/**
 * ReduceByKeyOperator is the class that implements the reduce functionality.
 * It takes in the key and value extractors, an initial value, and a reduceFunction
 * function as the input. The result are key value pairs of <K, T>.
 * R: Input data type, K, Key type, T, Value type.
 * TODO: make it stateful or create a new stateful operator. The tricky part is how
 * to convert K and T to State<> which needs to be serializable.
 */
public class GeneralReduceByKeyOperator<R, K, T> extends StreamletOperator<R, KeyValue<K, T>> {
  private SerializableFunction<R, K> keyExtractor;
  private T identity;
  private SerializableBiFunction<T, R, ? extends T> reduceFn;

  private Map<K, T> reduceMap;

  public GeneralReduceByKeyOperator(SerializableFunction<R, K> keyExtractor,
                                    T identity,
                                    SerializableBiFunction<T, R, ? extends T> reduceFn) {
    this.keyExtractor = keyExtractor;
    this.identity = identity;
    this.reduceFn = reduceFn;
    reduceMap = new HashMap<K, T>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    K key = keyExtractor.apply(obj);

    T oldValue = reduceMap.getOrDefault(key, identity);
    T newValue = reduceFn.apply(oldValue, obj);

    reduceMap.put(key, newValue);
    collector.emit(new Values(new KeyValue<K, T>(key, newValue)));
    collector.ack(tuple);
  }
}
