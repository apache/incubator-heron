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

package org.apache.heron.common.basics;

import java.util.HashMap;
import java.util.Map;

public final class CPUShare extends ResourceMeasure<Double> {

  private CPUShare(Double value) {
    super(value);
  }

  public static CPUShare fromDouble(double value) {
    return new CPUShare(value);
  }

  @Override
  public CPUShare minus(ResourceMeasure<Double> other) {
    return new CPUShare(value - other.value);
  }

  @Override
  public CPUShare plus(ResourceMeasure<Double> other) {
    return new CPUShare(value + other.value);
  }

  @Override
  public CPUShare multiply(int factor) {
    return new CPUShare(value * factor);
  }

  @Override
  public CPUShare divide(int factor) {
    return new CPUShare(value / factor);
  }

  @Override
  public CPUShare increaseBy(int percentage) {
    return new CPUShare(value * (1.0 + percentage / 100.0));
  }

  public static Map<String, CPUShare> convertDoubleMapToCpuShareMap(Map<String, Double> doubleMap) {
    Map<String, CPUShare> retval = new HashMap<>();
    for (Map.Entry<String, Double> entry : doubleMap.entrySet()) {
      retval.put(entry.getKey(), new CPUShare(entry.getValue()));
    }
    return retval;
  }

  @Override
  public String toString() {
    return String.format("CPUShare{%.3f}", value);
  }
}
