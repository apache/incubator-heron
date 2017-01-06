//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.scheduler.dryrun;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.Resource;

public class UpdateDryRunRender extends DryRunRender {

  private final UpdateDryRunResponse response;

  public UpdateDryRunRender(UpdateDryRunResponse response) {
    this.response = response;
  }

  public String renderTable() {
    Map<String, Resource> newComponentsResource =
        componentsResource(response.getPackingPlan());
    Map<String, Resource> oldComponentsResource =
        componentsResource(response.getOldPackingPlan());
    Map<String, Integer> newComponentsContainersNum =
        componentsParallelism(response.getPackingPlan());
    Map<String, Integer> oldComponentsContainersNum =
        componentsParallelism(response.getOldPackingPlan());
    List<List<String>> rows = new ArrayList<>();
    for (Map.Entry<String, Resource> entry: newComponentsResource.entrySet()) {
      String componentName = entry.getKey();
      Resource newResource = entry.getValue();
      Resource oldResource = oldComponentsResource.get(componentName);
      int newContainerNum = newComponentsContainersNum.get(componentName);
      int oldContainerNum = oldComponentsContainersNum.get(componentName);
      int newTotalCpu = (int) newResource.getCpu();
      int oldTotalCpu = (int) oldResource.getCpu();
      long newTotalRam = newResource.getRam().asGigabytes();
      long oldTotalRam = oldResource.getRam().asGigabytes();
      long newTotalDisk = newResource.getDisk().asGigabytes();
      long oldTotalDisk = oldResource.getDisk().asGigabytes();
      rows.add(Arrays.asList(
          componentName,
          formatChange(oldTotalCpu, newTotalCpu),
          formatChange(oldTotalDisk, newTotalDisk),
          formatChange(oldTotalRam, newTotalRam),
          formatChange(oldContainerNum, newContainerNum)));
    }
    return createTable(rows);
  }

  public String renderRaw() {
    StringBuilder builder = new StringBuilder();
    String topologyName = response.getTopology().getName();
    String packingClassName = Context.packingClass(response.getConfig());
    builder.append("Packing class: " + packingClassName + "\n");
    builder.append("Topology: " + topologyName + "\n");
    builder.append("Packing plan:\n");
    builder.append(response.getPackingPlan().toString() + "\n");
    builder.append("Old packing plan:\n");
    builder.append(response.getOldPackingPlan().toString() + "\n");
    return builder.toString();
  }
}
