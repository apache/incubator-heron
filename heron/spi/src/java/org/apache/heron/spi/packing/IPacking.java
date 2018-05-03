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

package org.apache.heron.spi.packing;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;
import org.apache.heron.spi.common.Config;

/**
 * Packing algorithm to use for packing multiple instances into containers. Packing hints like
 * number of containers may be passed through scheduler config. Configs might contain settings for
 * the number of containers requested or the amount of instance or container resources to allocate.
 * Implementations may choose to ignore some or all of these settings as needed, since they could be
 * contradictory, or not all possible to be achieved based on the algorithm.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public interface IPacking extends AutoCloseable {

  /**
   * Initialize the packing algorithm with the static config and the topology
   */
  void initialize(Config config, TopologyAPI.Topology topology);

  /**
   * Called by scheduler to generate container packing.
   * Packing algorithm output generates instance id and container id.
   *
   * @return PackingPlan describing the job to schedule.
   * @throws PackingException if the packing plan can not be generated
   */
  PackingPlan pack() throws PackingException;

  /**
   * This is to for disposing or cleaning up any internal state accumulated by
   * the uploader
   * <p>
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();
}
