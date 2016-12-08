//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.slamgr.cache;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;

public // Timeseries of metrics.
class TimeBucket {
  // A list of metrics each accumulated inside the time
  // that this bucket represents
  public LinkedList<String> data_;
  // Whats the start and end time that this TimeBucket contains metrics for
  public int start_time_;
  public int end_time_;

  // in seconds
  TimeBucket(int bucket_interval) {
    start_time_ = (int)Instant.now().getEpochSecond();
    end_time_ = start_time_ + bucket_interval;

    data_ = new LinkedList<>();
  }

  boolean overlaps(long start_time, long end_time) {
    return start_time_ <= end_time && start_time <= end_time_;
  }

  double aggregate() {
    if (data_.isEmpty()) {
      return 0;
    } else {
      double total = 0;
      for (String s : data_) {
        total += Double.parseDouble(s);
      }
      return total;
    }
  }

  long count() {
    return data_.size();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("start_time: " + start_time_ + "; ").append("end_time: " + end_time_ + "; ")
        .append("data: ").append(Arrays.toString(data_.toArray()));
    return sb.toString();
  }
}
