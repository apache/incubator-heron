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
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.*;
import com.twitter.heron.scheduler.dryrun.FormatterUtils.*;

public class SubmitDryRunRender implements DryRunRender {

  private final SubmitDryRunResponse response;
  private final PackingPlan plan;

  public SubmitDryRunRender(SubmitDryRunResponse response) {
    this.response = response;
    this.plan = response.getPackingPlan();
  }

  private class TableRender {
    private final PackingPlan plan;

    public TableRender(PackingPlan plan) {
      this.plan = plan;
    }

    public String render() {
      StringBuilder builder = new StringBuilder();
      Map<Integer, ContainerPlan> containersMap = plan.getContainersMap();
      for(Integer containerId: containersMap.keySet()) {
        StringBuilder containerBuilder = new StringBuilder();
        String header =
            new Cell(String.format("Container %d", containerId), TextStyle.BOLD).toString();
        containerBuilder.append(header);
        builder.append(header + "\n");
        ContainerPlan plan = containersMap.get(containerId);
        builder.append(FormatterUtils.renderResourceUsage(plan.getRequiredResource()) + "\n");
        List<Row> rows = new ArrayList<>();
        for(InstancePlan instancePlan: plan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(instancePlan,
              TextColor.DEFAULT, TextStyle.DEFAULT));
        }
        String containerTable = FormatterUtils.renderOneContainer(rows);
        builder.append(containerTable + "\n");
      }
      return builder.toString();
    }
  }

  public String renderTable() {
    return new TableRender(plan).render();
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
}