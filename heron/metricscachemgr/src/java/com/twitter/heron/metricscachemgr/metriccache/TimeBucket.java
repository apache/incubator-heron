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
package com.twitter.heron.metricscachemgr.metriccache;

import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;

// Timeseries of metrics.
public class TimeBucket {
  // A list of metrics each accumulated inside the time
  // that this bucket represents
  public LinkedList<String> data;
  // Whats the start and end time that this TimeBucket contains metrics for
  public int startTime;
  public int endTime;

  // in seconds
  TimeBucket(int bucketInterval) {
    startTime = (int) Instant.now().getEpochSecond();
    endTime = startTime + bucketInterval;

    data = new LinkedList<>();
  }

  boolean overlaps(long startTime1, long endTime1) {
    return this.startTime <= endTime1 && startTime1 <= this.endTime;
  }

  double aggregate() {
    if (data.isEmpty()) {
      return 0;
    } else {
      double total = 0;
      for (String s : data) {
        total += Double.parseDouble(s);
      }
      return total;
    }
  }

  long count() {
    return data.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("startTime: " + startTime + "; ").append("endTime: " + endTime + "; ")
        .append("data: ").append(Arrays.toString(data.toArray()));
    return sb.toString();
  }
}
