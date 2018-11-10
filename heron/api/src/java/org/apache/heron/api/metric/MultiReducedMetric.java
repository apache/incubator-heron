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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * A reduce metric that can hold multiple scoped values.
 * @param <T> accumulator to hold and update state
 * @param <U> type of input that can be handled
 * @param <V> type of reduced value
 */
public class MultiReducedMetric<T, U, V> implements IMetric<Map<String, V>> {
  private Map<String, ReducedMetric<T, U, V>> value = new ConcurrentHashMap<>();
  private IReducer<T, U, V> reducer;

  public MultiReducedMetric(IReducer<T, U, V> reducer) {
    this.reducer = reducer;
  }

  public ReducedMetric<T, U, V> scope(String key) {
    if (value.get(key) == null) {
      value.put(key, new ReducedMetric<>(reducer));
    }
    return value.get(key);
  }

  @Override
  public Map<String, V> getValueAndReset() {
    Map<String, V> ret = new HashMap<>();
    for (String key : value.keySet()) {
      V val = value.get(key).getValueAndReset();
      if (val != null) {
        ret.put(key, val);
      }
    }
    return ret;
  }
}
