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

package com.twitter.heron.api.windowing;

import java.io.Serializable;
import java.util.Map;

/**
 * Triggers the window calculations based on the policy.
 *
 * @param <T> the type of the event that is tracked
 * @param <S> the type of state that is used
 */
public interface TriggerPolicy<T extends Serializable, S> {
  /**
   * Tracks the event and could use this to invoke the trigger.
   *
   * @param event the input event
   */
  void track(Event<T> event);

  /**
   * resets the trigger policy.
   */
  void reset();

  /**
   * Starts the trigger policy. This can be used
   * during recovery to start the triggers after
   * recovery is complete.
   */
  void start();

  /**
   * Any clean up could be handled here.
   */
  void shutdown();

  /**
   * Return runtime state to be checkpointed by the framework for restoring the trigger policy
   * in case of failures.
   *
   * @return the state
   */
  S getState();

  /**
   * Restore the trigger policy from the state that was earlier checkpointed by the framework.
   *
   * @param state the state
   */
  void restoreState(S state);

  /**
   * Set the eviction policy to whatever eviction policy to use this with
   *
   * @param evictionPolicy the eviction policy
   */
  void setEvictionPolicy(EvictionPolicy<T, ?> evictionPolicy);

  /**
   * Set the trigger handler for this trigger policy to trigger
   *
   * @param triggerHandler the trigger handler
   */
  void setTriggerHandler(TriggerHandler triggerHandler);

  /**
   * Sets the window manager that uses this trigger policy
   *
   * @param windowManager the window manager
   */
  void setWindowManager(WindowManager<T> windowManager);

  /**
   * Sets the Config used for this topology
   *
   * @param config the configuration policy
   */
  void setTopologyConfig(Map<String, Object> config);
}
