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


import java.io.Serializable;

/**
 * Window is a container containing information about a particular window.
 * Transformations that depend on Windowing, pass the window information
 * inside their streamlets using this container.
 */
public final class Window implements Serializable {
  private static final long serialVersionUID = 5103471810104775854L;
  private long startTimeMs;
  private long endTimeMs;
  private long count;

  Window() {
    // nothing really
  }

  public Window(long startTimeMs, long endTimeMs, long count) {
    this.startTimeMs = startTimeMs;
    this.endTimeMs = endTimeMs;
    this.count = count;
  }

  public long getStartTime() {
    return startTimeMs;
  }
  public long getEndTime() {
    return endTimeMs;
  }
  public long getCount() {
    return count;
  }

  @Override
  public String toString() {
    return "{WindowStart: " + String.valueOf(startTimeMs) + " WindowEnd: "
           + String.valueOf(endTimeMs) + " Count: " + String.valueOf(count) + " }";
  }
}
