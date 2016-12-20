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

import java.util.Map;

import com.twitter.heron.spi.packing.PackingPlan;

public class UpdateDryRunResponse extends SubmitDryRunResponse {
  private static final long serialVersionUID = 18244716698543219L;

  private final PackingPlan oldPlan;
  private final Map<String, Integer> changeRequests;

  public UpdateDryRunResponse(String topologyName, PackingPlan oldPlan,
                              PackingPlan newPlan, Map<String, Integer> changeRequests,
                              String packingClass) {
    super(topologyName, newPlan, packingClass);
    this.oldPlan = oldPlan;
    this.changeRequests = changeRequests;
  }

  public PackingPlan getOldPackingPlan() {
    return oldPlan;
  }

  public Map<String, Integer> getChangeRequests() {
    return changeRequests;
  }
}
