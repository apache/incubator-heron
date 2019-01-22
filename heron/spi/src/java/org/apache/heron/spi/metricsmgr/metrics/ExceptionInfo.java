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

package org.apache.heron.spi.metricsmgr.metrics;

/**
 * An immutable class providing a view of ExceptionInfo
 */
public class ExceptionInfo {
  private final String stackTrace;
  private final String lastTime;
  private final String firstTime;
  private final int count;
  private final String logging;

  public ExceptionInfo(String stackTrace, String lastTime, String firstTime,
                       int count, String logging) {
    this.stackTrace = stackTrace;
    this.lastTime = lastTime;
    this.firstTime = firstTime;
    this.count = count;
    this.logging = logging;
  }

  /**
   * Get stack trace of exception. First two lines of stack trace is used for aggregating exception.
   *
   * @return Stack trace of exception
   */
  public String getStackTrace() {
    return stackTrace;
  }

  /**
   * Get last time the exception occurred in the metrics collection interval
   *
   * @return Last Time the exception occurred
   */
  public String getLastTime() {
    return lastTime;
  }

  /**
   * Get first time the exception occurred in the metrics collection interval
   *
   * @return First time the exception occurred
   */
  public String getFirstTime() {
    return firstTime;
  }

  /**
   * Get number of time exception occurred in the metrics collection interval
   *
   * @return Number of time exception occurred
   */
  public int getCount() {
    return count;
  }

  /**
   * Get additional text logged.
   *
   * @return Additional text logged.
   */
  public String getLogging() {
    return logging;
  }

  @Override
  public String toString() {
    return String.format(
        "{stack trace = %s, last time = %s, first time = %s, count = %d, logging = %s}",
        getStackTrace(), getLastTime(), getFirstTime(), getCount(), getLogging());
  }
}
