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
package com.twitter.heron.scheduler.utils;

import com.twitter.heron.spi.packing.PackingPlan;

/**
 * Dry-run response, which extends RuntimeException because it is easy
 * to propagate up
 */
public class DryRunResponse extends RuntimeException {
  private static final long serialVersionUID = 999911708859944856L;

  private final String topologyName;
  private final PackingPlan packingPlan;

  public DryRunResponse(String topologyName, PackingPlan packingPlan) {
    this.topologyName = topologyName;
    this.packingPlan = packingPlan;
  }

}
