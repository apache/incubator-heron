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

import java.util.Map;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;
import org.apache.heron.spi.common.Config;

/**
 * Packing algorithm for adding and/or removing component instances from an existing packing plan
 * Packing hints like number of containers may be passed through scheduler config.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public interface IRepacking extends AutoCloseable {

  /**
   * Initialize the packing algorithm with the static config and the associated topology
   * @param config topology config
   * @param topology topology to repack
   */
  void initialize(Config config, TopologyAPI.Topology topology);

  /**
   * Generates a new packing given an existing packing and component changes
   * Packing algorithm output generates instance id and container id.
   * @param currentPackingPlan Existing packing plan
   * @param componentChanges Map &lt; componentName, new component parallelism &gt;
   * that contains the parallelism for each component whose parallelism has changed.
   * @return PackingPlan describing the new packing plan.
   * @throws PackingException if the packing plan can not be generated
   */
  PackingPlan repack(PackingPlan currentPackingPlan,
                     Map<String, Integer> componentChanges) throws PackingException;

  /**
   * Generates a new packing given an existing packing and component changes
   * Packing algorithm output generates instance id and container id.
   * @param currentPackingPlan Existing packing plan
   * @param componentChanges Map &lt; componentName, new component parallelism &gt;
   * that contains the parallelism for each component whose parallelism has changed.
   * @param containers &lt; the new number of containers for the topology
   * specified by the user
   * @return PackingPlan describing the new packing plan.
   * @throws PackingException if the packing plan can not be generated
   */
  PackingPlan repack(PackingPlan currentPackingPlan, int containers,
                     Map<String, Integer> componentChanges) throws PackingException;

  /**
   * This is to for disposing or cleaning up any internal state.
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   */
  void close();
}
