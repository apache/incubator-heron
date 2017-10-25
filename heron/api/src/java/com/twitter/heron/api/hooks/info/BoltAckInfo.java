// Copyright 2017 Twitter. All rights reserved.
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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.twitter.heron.api.hooks.info;

import java.time.Duration;

import com.twitter.heron.api.tuple.Tuple;

public class BoltAckInfo {
  private Tuple tuple;
  private int ackingTaskId;
  private Duration processLatency; // null if it wasn't sampled

  public BoltAckInfo(Tuple tuple, int ackingTaskId, Duration processLatency) {
    this.tuple = tuple;
    this.ackingTaskId = ackingTaskId;
    this.processLatency = processLatency;
  }

  public Tuple getTuple() {
    return tuple;
  }

  public int getAckingTaskId() {
    return ackingTaskId;
  }

  public Duration getProcessLatency() {
    return processLatency;
  }
}
