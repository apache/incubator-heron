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

public class MultiAssignableMetric<T extends Number> implements IMetric<Map<String, T>> {
  private final Map<String, AssignableMetric<T>> value = new ConcurrentHashMap<>();
  private T initialValue;

  public MultiAssignableMetric(T initialValue) {
    this.initialValue = initialValue;
  }

  public AssignableMetric<T> scope(String key) {
    AssignableMetric<T> val = value.get(key);
    if (val == null) {
      value.put(key, val = new AssignableMetric<>(this.initialValue));
    }
    return val;
  }

  public AssignableMetric<T> safeScope(String key) {
    AssignableMetric<T> val;
    synchronized (value) {
      val = value.get(key);
      if (val == null) {
        value.put(key, val = new AssignableMetric<>(this.initialValue));
      }
    }
    return val;
  }

  @Override
  public Map<String, T> getValueAndReset() {
    Map<String, T> ret = new HashMap<>();
    synchronized (value) {
      for (Map.Entry<String, AssignableMetric<T>> e : value.entrySet()) {
        ret.put(e.getKey(), e.getValue().getValueAndReset());
      }
    }
    return ret;
  }
}
