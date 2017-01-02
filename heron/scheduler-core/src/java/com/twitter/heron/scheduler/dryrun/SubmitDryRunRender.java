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
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

public class SubmitDryRunRender extends DryRunRender {

  private final SubmitDryRunResponse response;

  public SubmitDryRunRender(SubmitDryRunResponse response) {
    this.response = response;
  }

  public String renderTable() {
    Map<String, Resource> componentsResource =
        this.componentsResource(response.getPackingPlan());
    Map<String, Integer> componentsParallelism =
        this.componentsParallelism(response.getPackingPlan());
    List<List<String>> rows = new ArrayList<>();
    for (Map.Entry<String, Resource> entry: componentsResource.entrySet()) {
      String componentName = entry.getKey();
      Resource resource = entry.getValue();
      int totalCpu = (int) resource.getCpu();
      long totalRam = resource.getRam().asGigabytes();
      long totalDisk = resource.getDisk().asGigabytes();
      int containerNum = componentsParallelism.get(componentName);
      rows.add(Arrays.asList(
          componentName, String.valueOf(totalCpu), String.valueOf(totalDisk),
          String.valueOf(totalRam), String.valueOf(containerNum)));
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
    builder.append(response.getPackingPlan().toString());
    return builder.toString();
  }

  /*
  @Override
  public String render(UpdateDryRunResponse response) {
    StringBuilder builder = new StringBuilder();
    builder.append(common(response));
    builder.append('\n');
    builder.append("Old packing plan:\n");
    builder.append(response.getOldPackingPlan().toString());
    return builder.toString();
  } */

}