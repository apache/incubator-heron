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

package org.apache.heron.streamlet.impl.windowings;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.streamlet.WindowConfig;

/**
 * CustomWindowConfig implements a trigger/eviction based WindowConfig.
 */
public final class CustomWindowConfig implements WindowConfig {
  private TriggerPolicy<Tuple, ?> triggerPolicy;
  private EvictionPolicy<Tuple, ?> evictionPolicy;

  public CustomWindowConfig(TriggerPolicy<Tuple, ?> triggerPolicy,
                          EvictionPolicy<Tuple, ?> evictionPolicy) {
    this.triggerPolicy = triggerPolicy;
    this.evictionPolicy = evictionPolicy;
  }

  /**
   * Apply this WindowConfig object to a bolt object
   * @param bolt the target bolt object
   */
  @Override
  public void applyTo(BaseWindowedBolt bolt) {
    bolt.withCustomEvictor(evictionPolicy);
    bolt.withCustomTrigger(triggerPolicy);
  }

  @Override
  public void Dummy() { }
}
