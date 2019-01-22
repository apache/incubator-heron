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

package org.apache.heron.api.metric;

/**
 * Apply an update to an metric using an IReducer for which a result can be extracted.
 * @param <T> accumulator to hold and update state
 * @param <U> type of input that can be handled
 * @param <V> type of reduced value
 */
public class ReducedMetric<T, U, V> implements IMetric<V> {
  private final IReducer<T, U, V> reducer;
  private T accumulator;

  public ReducedMetric(IReducer<T, U, V> aReducer) {
    reducer = aReducer;
    accumulator = reducer.init();
  }

  public void update(U value) {
    accumulator = reducer.reduce(accumulator, value);
  }

  @Override
  public V getValueAndReset() {
    V ret = reducer.extractResult(accumulator);
    accumulator = reducer.init();
    return ret;
  }
}
