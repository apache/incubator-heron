// Copyright 2016 Twitter. All rights reserved.
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
package com.twitter.heron.scheduler.dryrun;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.packing.PackingPlan;

public abstract class DryRunResponse extends RuntimeException {

  private static final long serialVersionUID = 4875372144841447L;
  private final TopologyAPI.Topology topology;
  private final Config config;
  private final PackingPlan packingPlan;

  public DryRunResponse(TopologyAPI.Topology topology, Config config, PackingPlan packingPlan) {
    this.topology = topology;
    this.config = config;
    this.packingPlan = packingPlan;
  }

  public TopologyAPI.Topology getTopology() {
    return topology;
  }

  public Config getConfig() {
    return config;
  }

  public PackingPlan getPackingPlan() {
    return packingPlan;
  }
}
