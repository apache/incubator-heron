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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  private Map<String, List<T>> output;    // Stream id to output data to be emitted.
  private boolean successful;             // If the execution succeeded?
  private boolean anchored;               // If anchors should be added when emitting tuples?

  // Disable constructor. User should use the static functions below to create objects
  private CustomOperatorOutput(Map<String, List<T>> data) {
    this.output = data;
    this.successful = data != null;
    this.anchored = true;
  }

  /**
   * Get collected data
   * @return data to be emitted. The data is a map of stream id to list of objects
   */
  public Map<String, List<T>> getData() {
    return output;
  }

  /**
   * Check successful flag
   * @return true if the execution succeeded. If not successful, fail() will be called
   * instead of ack() in bolt
   */
  public boolean isSuccessful() {
    return successful;
  }

  /**
   * Check anchored flag
   * @return true if the output data needs to be anchored when emitting
   */
  public boolean isAnchored() {
    return anchored;
  }

  /**
   * If the output data needs to be anchored or not
   * @param flag the anchored flag
   * @return this object itself
   */
  public CustomOperatorOutput<T> withAnchor(boolean flag) {
    anchored = flag;
    return this;
  }

  /* Util static functions. Users should use them to create objects */
  public static <R> CustomOperatorOutput<R> create() {
    Map<String, List<R>> dataMap = new HashMap<String, List<R>>();
    return new CustomOperatorOutput<R>(dataMap);
  }

  /**
   * Generate a CustomOperatorOutput object with empty output
   * @return a CustomOperatorOutput object with empty output
   */
  public static <R> CustomOperatorOutput<R> succeed() {
    Map<String, List<R>> dataMap = new HashMap<String, List<R>>();
    return succeed(dataMap);
  }

  /**
   * Generate a CustomOperatorOutput object with a single object in the default stream
   * @param data the data object to be added
   * @return the generated CustomOperatorOutput object
   */
  public static <R> CustomOperatorOutput<R> succeed(R data) {
    return succeed(Arrays.asList(data));
  }

  /**
   * Generate a CustomOperatorOutput object with a list of output objects
   * in the default stream
   * @param data the list of data to be added
   * @return the generated CustomOperatorOutput object
   */
  public static <R> CustomOperatorOutput<R> succeed(List<R> data) {
    Map<String, List<R>> dataMap = new HashMap<String, List<R>>();
    dataMap.put(Utils.DEFAULT_STREAM_ID, data);
    return succeed(dataMap);
  }

  /**
   * Generate a CustomOperatorOutput object with a map of stream id to list of output objects
   * @param data the output data in a map of stream id to list of output objects
   * @return the generated CustomOperatorOutput object
   */
  public static <R> CustomOperatorOutput<R> succeed(Map<String, List<R>> data) {
    CustomOperatorOutput<R> retval = new CustomOperatorOutput<R>(data);
    return retval;
  }

  /**
   * Generate a result that represents a fail result
   * @return a result that represents a fail result
   */
  public static <R> CustomOperatorOutput<R> fail() {
    CustomOperatorOutput<R> failed = new CustomOperatorOutput<R>(null);
    return failed;
  }
}
