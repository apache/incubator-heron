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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.SerializableBinaryOperator;
import org.apache.heron.streamlet.SerializableFunction;

/**
 * ReduceByKeyOperator is the class that implements the reduce functionality.
 * It takes in the key and value extractors, an initial value, and a reduceFunction
 * function as the input. The result are key value pairs of <K, T>.
 * R: Input data type, K, Key type, T, Value type.
 * ReduceByKeyOperator is a stateful component and the current reduce map is the state.
 */
public class ReduceByKeyOperator<R, K extends Serializable, T extends Serializable>
    extends StreamletOperator<R, KeyValue<K, T>>
    implements IStatefulComponent<K, T> {

  private SerializableFunction<R, K> keyExtractor;
  private SerializableFunction<R, T> valueExtractor;
  private T identity;
  private SerializableBinaryOperator<T> reduceFn;

  private Map<K, T> state;  // It is a hashmap of <K, T>

  public ReduceByKeyOperator(SerializableFunction<R, K> keyExtractor,
                             SerializableFunction<R, T> valueExtractor,
                             T identity,
                             SerializableBinaryOperator<T> reduceFn) {
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.identity = identity;
    this.reduceFn = reduceFn;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    R obj = (R) tuple.getValue(0);
    K key = keyExtractor.apply(obj);
    T value = valueExtractor.apply(obj);

    T currentValue = state.getOrDefault(key, identity);
    T newValue = reduceFn.apply(currentValue, value);

    state.put(key, newValue);
    collector.emit(new Values(new KeyValue<K, T>(key, newValue)));
    collector.ack(tuple);
  }

  // For stateful process
  @Override
  public void initState(State<K, T> startupState) {
    if (startupState == null) {
      this.state = (Map<K, T>) startupState;
    } else {
      this.state = new HashMap<K, T>();
    }
  }

  @Override
  public void preSave(String checkpointId) {
  }

  @Override
  public void cleanup() {
  }
}
