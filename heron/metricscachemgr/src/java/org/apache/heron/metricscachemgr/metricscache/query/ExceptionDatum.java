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

package org.apache.heron.metricscachemgr.metricscache.query;

import org.apache.heron.metricscachemgr.metricscache.store.ExceptionDatapoint;

/**
 * immutable data bag for exception datum
 */
public final class ExceptionDatum {
  // Source of exception.
  private final String componentName;
  // In case of a regular instance, it is the instance's
  // instance_id. For stmgr it is the stmgr_id
  private final String instanceId;

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

  public ExceptionDatum(String componentName, String instanceId, String hostname,
                        String stackTrace, String lastTime, String firstTime,
                        int count, String logging) {
    this.componentName = componentName;
    this.instanceId = instanceId;
    this.hostname = hostname;
    this.stackTrace = stackTrace;
    this.lastTime = lastTime;
    this.firstTime = firstTime;
    this.count = count;
    this.logging = logging;
  }

  public ExceptionDatum(ExceptionDatum exceptionDatum) {
    this(exceptionDatum.componentName, exceptionDatum.instanceId, exceptionDatum.hostname,
        exceptionDatum.stackTrace, exceptionDatum.lastTime, exceptionDatum.firstTime,
        exceptionDatum.count, exceptionDatum.logging);
  }

  public ExceptionDatum(String componentName, String instanceId,
                        ExceptionDatapoint exceptionDatapoint) {
    this.componentName = componentName;
    this.instanceId = instanceId;
    this.hostname = exceptionDatapoint.getHostname();
    this.stackTrace = exceptionDatapoint.getStackTrace();
    this.lastTime = exceptionDatapoint.getLastTime();
    this.firstTime = exceptionDatapoint.getFirstTime();
    this.count = exceptionDatapoint.getCount();
    this.logging = exceptionDatapoint.getLogging();
  }

  public String getComponentName() {
    return componentName;
  }

  public String getInstanceId() {
    return instanceId;
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
