/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.windowing.triggers;

import java.io.Serializable;
import java.util.Map;

import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TriggerHandler;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.api.windowing.WindowManager;


public abstract class AbstractBaseTriggerPolicy<T extends Serializable, S>
    implements TriggerPolicy<T, S> {
  protected TriggerHandler handler;
  protected EvictionPolicy<T, ?> evictionPolicy;
  protected WindowManager<T> windowManager;
  protected Boolean started;
  protected Map<String, Object> topoConf;

  private boolean requiresEvictionPolicy = false;
  private boolean requiresWindowManager = false;
  private boolean requiresTopologyConfig = false;

  /**
  * Set the requirements in the constructor
  */
  public AbstractBaseTriggerPolicy() {
  }

  /**
  * Set the eviction policy to whatever eviction policy to use this with
  *
  * @param evictionPolicy the eviction policy
  */
  public void setEvictionPolicy(EvictionPolicy<T, ?> evictionPolicy) {
    this.evictionPolicy = evictionPolicy;
  }

  /**
  * Set the trigger handler for this trigger policy to trigger
  *
  * @param triggerHandler the trigger handler
  */
  public void setTriggerHandler(TriggerHandler triggerHandler) {
    this.handler = triggerHandler;
  }

  /**
  * Sets the window manager that uses this trigger policy
  *
  * @param windowManager the window manager
  */
  public void setWindowManager(WindowManager<T> windowManager) {
    this.windowManager = windowManager;
  }

  /**
  * Sets the Config used for this topology
  *
  * @param config the configuration object
  */
  public void setTopologyConfig(Map<String, Object> config) {
    this.topoConf = config;
  }

  /**
  * Starts the trigger policy. This can be used
  * during recovery to start the triggers after
  * recovery is complete.
  */
  @Override
  public void start() {
    started = true;
  }
}
