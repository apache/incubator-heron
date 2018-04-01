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
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.heron.api.windowing.DefaultEvictionContext;
import com.twitter.heron.api.windowing.Event;
import com.twitter.heron.api.windowing.TriggerHandler;

/**
 * A trigger that tracks event counts and calls back {@link TriggerHandler#onTrigger()}
 * when the count threshold is hit.
 *
 * @param <T> the type of event tracked by this policy.
 */
public class CountTriggerPolicy<T extends Serializable> extends
        AbstractBaseTriggerPolicy<T, Integer> {
  private final int count;
  private final AtomicInteger currentCount;

  public CountTriggerPolicy(int count) {
    super();

    this.count = count;
    this.currentCount = new AtomicInteger();
  }

  @Override
  public void track(Event<T> event) {
    if (started && !event.isWatermark()) {
      if (currentCount.incrementAndGet() >= count) {
        evictionPolicy.setContext(new DefaultEvictionContext(System.currentTimeMillis()));
        handler.onTrigger();
      }
    }
  }

  @Override
  public void reset() {
    currentCount.set(0);
  }

  @Override
  public void shutdown() {
    // NOOP
  }

  @Override
  public Integer getState() {
    return currentCount.get();
  }

  @Override
  public void restoreState(Integer state) {
    currentCount.set(state);
  }

  @Override
  public String toString() {
    return "CountTriggerPolicy{" + "count=" + count + ", currentCount=" + currentCount
        + ", started=" + started + '}';
  }
}
