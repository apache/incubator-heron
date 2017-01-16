// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.metricscachemgr.metricscache.datapoint;

/**
 * exception data
 * TODO(huijun) object pool to avoid java gc
 */
public class ExceptionDatapoint {
  // Source of exception.
  public String componentName;
  // Current hostname.
  public String hostname;
  // In case of a regular instance, it is the instance's
  // instance_id. For stmgr it is the stmgr_id
  public String instanceId;

  // Stack trace of exception. First two lines of stack trace is used for aggregating exception.
  public String stacktrace;
  // Last time the exception occurred in the metrics collection interval
  public String lasttime;
  // First time the exception occurred in the metrics collection interval
  public String firsttime;
  // Number of time exception occurred in the metrics collection interval
  public int count;
  // Additional text logged.
  public String logging;

}
