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
 * metric datum with timestamp
 * TODO(huijun) object pool to avoid java gc
 */
public class MetricDatapoint {
  public long timestamp = 0;
  // one data point, compatible with protobuf message from sink
  public String value = null;


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("(").append(timestamp).append(", ").append(value).append(")");
    return sb.toString();
  }

}
