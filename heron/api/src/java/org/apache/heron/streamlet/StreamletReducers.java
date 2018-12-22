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

package org.apache.heron.streamlet;

/**
 * This class contains a few standard reduces that can be used with
 * Streamlet reduce functions such as reduceByKeyAndWindow.
 * Example, assuming s is a Stringlet<T> object and each tuple has these functions:
 *   - Integer getKey() and
 *   - Double getValue()
 * To get streams of sum, min and max of all values upto the current one:
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::sum);
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::min);
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::max);
 */
public final class StreamletReducers {
  // This is a utility class and shouldn't have public constructor.
  private StreamletReducers() {
  }

  public static Integer sum(Integer a, Integer b) {
    return a + b;
  }

  public static Long sum(Long a, Long b) {
    return a + b;
  }

  public static Float sum(Float a, Float b) {
    return a + b;
  }

  public static Double sum(Double a, Double b) {
    return a + b;
  }

  public static Integer max(Integer a, Integer b) {
    return Math.max(a, b);
  }

  public static Long max(Long a, Long b) {
    return Math.max(a, b);
  }

  public static Float max(Float a, Float b) {
    return Math.max(a, b);
  }

  public static Double max(Double a, Double b) {
    return Math.max(a, b);
  }

  public static Integer min(Integer a, Integer b) {
    return Math.min(a, b);
  }

  public static Long min(Long a, Long b) {
    return Math.min(a, b);
  }

  public static Float min(Float a, Float b) {
    return Math.min(a, b);
  }

  public static Double min(Double a, Double b) {
    return Math.min(a, b);
  }
}
