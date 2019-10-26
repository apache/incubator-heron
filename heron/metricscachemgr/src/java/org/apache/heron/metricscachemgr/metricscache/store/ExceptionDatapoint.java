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

package org.apache.heron.metricscachemgr.metricscache.store;

/**
 * immutable exception data in store
 * TODO(huijun) object pool to avoid java gc
 */
public final class ExceptionDatapoint {
  // Current hostname.
  private final String hostname;

  // Stack trace of exception. First two lines of stack trace is used for aggregating exception.
  private final String stackTrace;
  // Last time the exception occurred in the metrics collection interval
  private final String lastTime;
  // First time the exception occurred in the metrics collection interval
  private final String firstTime;
  // Number of time exception occurred in the metrics collection interval
  private final int count;
  // Additional text logged.
  private final String logging;

  public ExceptionDatapoint(String hostname, String stackTrace, String lastTime, String firstTime,
                            int count, String logging) {
    this.hostname = hostname;
    this.stackTrace = stackTrace;
    this.lastTime = lastTime;
    this.firstTime = firstTime;
    this.count = count;
    this.logging = logging;
  }

  public String getHostname() {
    return hostname;
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public String getLastTime() {
    return lastTime;
  }

  public String getFirstTime() {
    return firstTime;
  }

  public int getCount() {
    return count;
  }

  public String getLogging() {
    return logging;
  }
}
