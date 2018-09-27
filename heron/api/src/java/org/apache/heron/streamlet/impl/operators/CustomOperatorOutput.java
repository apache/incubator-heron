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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.heron.api.utils.Utils;



/**
 * CustomOperatorOutput is the class for data returned by CustomOperators' process() function.
 * CustomStreamlet is responsible for converting the output to emit/ack/fail is lowlevel API.
 *
 * Usage:
 *   Assuming data is already stored in an integer array: data,
 *     return CustomOperatorOutput.apply(data);
 */
public final class CustomOperatorOutput<T> {
  private boolean anchored;                   // If anchors should be added when emitting tuples?
  private Map<String, Collection<T>> data;    // Output data to be emitted.

  // Disable constructor. User should use the static functions below to create objects
  private CustomOperatorOutput() {
  }

  public Map<String, Collection<T>> getData() {
    return data;
  }

  public boolean isAnchored() {
    return anchored;
  }

  /* Util static functions. Users should use them to create objects */
  public static <R> Optional<CustomOperatorOutput<R>> success() {
    Map<String, Collection<R>> dataMap = new HashMap<String, Collection<R>>();
    return success(dataMap, true);
  }

  public static <R> Optional<CustomOperatorOutput<R>> success(Collection<R> data) {
    return success(data, true);
  }

  public static <R> Optional<CustomOperatorOutput<R>> success(Collection<R> data,
                                                              boolean anchored) {
    Map<String, Collection<R>> dataMap = new HashMap<String, Collection<R>>();
    dataMap.put(Utils.DEFAULT_STREAM_ID, data);
    return success(dataMap, anchored);
  }

  public static <R> Optional<CustomOperatorOutput<R>> success(Map<String, Collection<R>> data) {
    return success(data, true);
  }

  public static <R> Optional<CustomOperatorOutput<R>> success(Map<String, Collection<R>> data,
                                                              boolean anchored) {
    CustomOperatorOutput<R> retval = new CustomOperatorOutput<R>();
    retval.data = data;
    retval.anchored = anchored;
    return Optional.of(retval);
  }

  public static <R> Optional<CustomOperatorOutput<R>> fail() {
    return Optional.empty();
  }
}
