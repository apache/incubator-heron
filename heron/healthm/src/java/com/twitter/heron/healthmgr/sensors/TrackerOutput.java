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


package com.twitter.heron.healthmgr.sensors;

import java.util.Map;

public class TrackerOutput {

  private String status;
  private int executiontime;
  private String message;
  private String version;
  private Result result;

  public TrackerOutput() {
  }

  public TrackerOutput(String status, int executiontime, String message, String version,
                       Result result) {
    this.status = status;
    this.executiontime = executiontime;
    this.message = message;
    this.version = version;
    this.result = result;
  }

  public String getStatus() {
    return status;
  }

  public int getExecutiontime() {
    return executiontime;
  }

  public String getMessage() {
    return message;
  }

  public Result getResult() {
    return result;
  }

  public String getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "TrackerOutput{"
        + "status='" + status + '\''
        + ", result=" + result
        + '}';
  }

  public static class Result {
    private Map<String, Map<String, String>> metrics;
    private int interval;
    private String component;

    public Result() {
    }

    public Result(Map<String, Map<String, String>> metrics, int interval, String component) {
      this.metrics = metrics;
      this.interval = interval;
      this.component = component;
    }

    public Map<String, Map<String, String>> getMetrics() {
      return metrics;
    }

    public int getInterval() {
      return interval;
    }

    public String getComponent() {
      return component;
    }

    @Override
    public String toString() {
      return "Result{"
          + "metrics=" + metrics
          + ", interval=" + interval
          + ", component='" + component + '\''
          + '}';
    }
  }
}
