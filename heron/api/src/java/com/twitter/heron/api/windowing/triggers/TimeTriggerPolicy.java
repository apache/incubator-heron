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

package com.twitter.heron.api.windowing.triggers;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.windowing.DefaultEvictionContext;
import com.twitter.heron.api.windowing.Event;
import com.twitter.heron.api.windowing.EvictionPolicy;
import com.twitter.heron.api.windowing.TriggerHandler;
import com.twitter.heron.api.windowing.TriggerPolicy;

/**
 * Invokes {@link TriggerHandler#onTrigger()} after the duration.
 */

public class TimeTriggerPolicy<T extends Serializable> implements TriggerPolicy<T, Void> {

  private long duration;
  private final TriggerHandler handler;
  private final EvictionPolicy<T, ?> evictionPolicy;
  private Map<String, Object> topoConf;


  public TimeTriggerPolicy(long millis, TriggerHandler handler) {
    this(millis, handler, null, new Config());
  }

  public TimeTriggerPolicy(long millis, TriggerHandler handler, EvictionPolicy<T, ?>
      evictionPolicy, Map<String, Object> topoConf) {
    this.duration = millis;
    this.handler = handler;
    this.evictionPolicy = evictionPolicy;
    this.topoConf = topoConf;
  }

  @Override
  public void track(Event<T> event) {

  }

  @Override
  public void reset() {

  }

  @Override
  public void start() {
    Config.registerTopologyTimerEvents(this.topoConf, "TimeTriggerPolicyTimer",
        Duration.ofMillis(this.duration), () -> triggerTask());
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String toString() {
    return "TimeTriggerPolicy{" + "duration=" + duration + '}';
  }

  private void triggerTask() {
    // do not process current timestamp since tuples might arrive while the trigger is executing
    long now = System.currentTimeMillis() - 1;

    /*
     * set the current timestamp as the reference time for the eviction policy
     * to evict the events
     */
    evictionPolicy.setContext(new DefaultEvictionContext(now, null, null, duration));
    handler.onTrigger();
  }

  @Override
  public Void getState() {
    return null;
  }

  @Override
  public void restoreState(Void state) {

  }
}
