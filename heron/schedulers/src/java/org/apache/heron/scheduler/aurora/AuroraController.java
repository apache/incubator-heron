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

package org.apache.heron.scheduler.aurora;

import java.util.Map;
import java.util.Set;

import org.apache.heron.spi.packing.PackingPlan;

/**
 * Interface that defines how a client interacts with aurora to control the job lifecycle
 */
public interface AuroraController {

  boolean createJob(Map<AuroraField, String> auroraProperties, Map<String, String> extra);
  boolean killJob();

  /**
   * Restarts a given container, or the entire job if containerId is null
   * @param containerId ID of container to restart, or entire job if null
   * @return the boolean return value
   */
  boolean restart(Integer containerId);

  void removeContainers(Set<PackingPlan.ContainerPlan> containersToRemove);
  Set<Integer> addContainers(Integer count);
}
