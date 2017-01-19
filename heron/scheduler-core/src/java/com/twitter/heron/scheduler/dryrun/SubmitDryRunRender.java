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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;

public class SubmitDryRunRender {

  private final SubmitDryRunResponse response;
  private final PackingPlan plan;

  public SubmitDryRunRender(SubmitDryRunResponse response) {
    this.response = response;
    this.plan = response.getPackingPlan();
  }

  private class TableRender implements DryRunRender {

    private final Config config;
    private final PackingPlan plan;

    TableRender(Config config, PackingPlan plan) {
      this.config = config;
      this.plan = plan;
    }

    public String render() {
      StringBuilder builder = new StringBuilder();
      Map<Integer, PackingPlan.ContainerPlan> containersMap = plan.getContainersMap();
      int numContainers = containersMap.size();
      builder.append(String.format("Total number of containers: %d", numContainers) + "\n");
      builder.append(String.format("Using packing class: %s", Context.packingClass(config)) + "\n");
      List<String> containerTables = new ArrayList<>();
      for (Integer containerId: containersMap.keySet()) {
        StringBuilder containerBuilder = new StringBuilder();
        String header =
            new FormatterUtils.Cell(String.format("Container %d", containerId),
                FormatterUtils.TextStyle.BOLD).toString();
        containerBuilder.append(header + "\n");
        PackingPlan.ContainerPlan containerPlan = containersMap.get(containerId);
        containerBuilder.append(FormatterUtils.renderResourceUsage(
            containerPlan.getRequiredResource()) + "\n");
        List<FormatterUtils.Row> rows = new ArrayList<>();
        for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(instancePlan,
              FormatterUtils.TextColor.DEFAULT, FormatterUtils.TextStyle.DEFAULT));
        }
        containerBuilder.append(FormatterUtils.renderOneContainer(rows));
        containerTables.add(containerBuilder.toString());
      }
      builder.append(String.join("\n", containerTables));
      return builder.toString();
    }
  }

  private class RawRender implements DryRunRender {

    private final SubmitDryRunResponse response;

    RawRender(SubmitDryRunResponse response) {
      this.response = response;
    }

    public String render() {
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

  public String renderTable() {
    return new TableRender(response.getConfig(), plan).render();
  }

  public String renderRaw() {
    return new RawRender(response).render();
  }
}
