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
import org.apache.heron.streamlet.impl.WindowConfigImpl;

/**
 * CountWindowConfig implements a count based WindowConfig.
 */
public final class CountWindowConfig extends WindowConfigImpl {
  private int windowSize;
  private int slideInterval;

  public CountWindowConfig(int windowSize, int slideInterval) {
    this.windowSize = windowSize;
    this.slideInterval = slideInterval;
  }

  @Override
  public void attachWindowConfig(BaseWindowedBolt bolt) {
    bolt.withWindow(BaseWindowedBolt.Count.of(windowSize),
                    BaseWindowedBolt.Count.of(slideInterval));
  }
}
