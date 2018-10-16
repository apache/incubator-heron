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

import java.util.Arrays;
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
  private Map<String, Collection<T>> output;    // Stream id to output data to be emitted.

  // Disable constructor. User should use the static functions below to create objects
  private CustomOperatorOutput(Map<String, Collection<T>> data) {
    this.anchored = true;
    this.output = data;
  }

  /**
   * Get collected data
   * @return data to be emitted. The data is a map of stream id to collection of objects
   */
  public Map<String, Collection<T>> getData() {
    return output;
  }

  /**
   * Check anchored flag
   * @return true if the output data needs to be anchored when emitting
   */
  public boolean isAnchored() {
    return anchored;
  }

  /**
   * Append a collection of data to the default output stream
   * @param newData collection of data to append
   * @return this object itself
   */
  public CustomOperatorOutput append(Collection<T> newData) {
    return append(Utils.DEFAULT_STREAM_ID, newData);
  }

  /**
   * Append a collection of data to the specified output stream
   * @param streamId the name of the output stream
   * @param newData collection of data to append
   * @return this object itself
   */
  public CustomOperatorOutput append(String streamId, Collection<T> newData) {
    if (output.containsKey(streamId)) {
      Collection<T> collection = output.get(streamId);
      collection.addAll(newData);
      output.put(streamId, newData);
    } else {
      output.put(streamId, newData);
    }

    return this;
  }

  /**
   * If the output data needs to be anchored or not
   * @param flag the anchored flag
   * @return this object itself
   */
  public CustomOperatorOutput withAnchor(boolean flag) {
    anchored = flag;
    return this;
  }

  /* Util static functions. Users should use them to create objects */

  /**
   * Generate a CustomOperatorOutput object with empty output
   * @return a CustomOperatorOutput object with empty output
   */
  public static <R> Optional<CustomOperatorOutput<R>> succeed() {
    Map<String, Collection<R>> dataMap = new HashMap<String, Collection<R>>();
    return succeed(dataMap);
  }

  /**
   * Generate a CustomOperatorOutput object with a single object in the default stream
   * @param data the data object to be added
   * @return the generated CustomOperatorOutput object
   */
  public static <R> Optional<CustomOperatorOutput<R>> succeed(R data) {
    return succeed(Arrays.asList(data));
  }

  /**
   * Generate a CustomOperatorOutput object with a collection of output objects
   * in the default stream
   * @param data the collection of data to be added
   * @return the generated CustomOperatorOutput object
   */
  public static <R> Optional<CustomOperatorOutput<R>> succeed(Collection<R> data) {
    Map<String, Collection<R>> dataMap = new HashMap<String, Collection<R>>();
    dataMap.put(Utils.DEFAULT_STREAM_ID, data);
    return succeed(dataMap);
  }

  /**
   * Generate a CustomOperatorOutput object with a map of stream id to collection of output objects
   * @param data the output data in a map of stream id to collection of output objects
   * @return the generated CustomOperatorOutput object
   */
  public static <R> Optional<CustomOperatorOutput<R>> succeed(Map<String, Collection<R>> data) {
    CustomOperatorOutput<R> retval = new CustomOperatorOutput<R>(data);
    return Optional.of(retval);
  }

  /**
   * Generate a result that represents a fail result (Optional.empty)
   * @return a result that represents a fail result (Optional.empty)
   */
  public static <R> Optional<CustomOperatorOutput<R>> fail() {
    return Optional.empty();
  }
}
