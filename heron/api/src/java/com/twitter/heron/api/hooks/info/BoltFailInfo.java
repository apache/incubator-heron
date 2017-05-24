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

package com.twitter.heron.api.hooks.info;

import java.time.Duration;

import com.twitter.heron.api.tuple.Tuple;

public class BoltFailInfo {
  private Tuple tuple;
  private int failingTaskId;
  private Duration failLatency; // null if it wasn't sampled

  public BoltFailInfo(Tuple tuple, int failingTaskId, Duration failLatency) {
    this.tuple = tuple;
    this.failingTaskId = failingTaskId;
    this.failLatency = failLatency;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public int getFailingTaskId() {
    return failingTaskId;
  }

  public Duration getFailLatency() {
    return failLatency;
  }
}
