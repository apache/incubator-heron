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

package org.apache.heron.scheduler.dryrun;

import java.util.Map;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.packing.PackingPlan;

public class UpdateDryRunResponse extends DryRunResponse {
  private static final long serialVersionUID = 18244716698543219L;

  private final PackingPlan oldPackingPlan;
  private final Map<String, Integer> changeRequests;

  public UpdateDryRunResponse(TopologyAPI.Topology topology, Config config,
                              PackingPlan newPackingPlan, PackingPlan oldPackingPlan,
                              Map<String, Integer> changeRequests) {
    super(topology, config, newPackingPlan);
    this.oldPackingPlan = oldPackingPlan;
    this.changeRequests = changeRequests;
  }

  public PackingPlan getOldPackingPlan() {
    return oldPackingPlan;
  }

  public Map<String, Integer> getChangeRequests() {
    return changeRequests;
  }
}
