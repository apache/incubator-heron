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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;

/**
 * Dry-run renderer that renders submit dry-run response in table format
 */
public class SubmitTableDryRunRenderer implements DryRunRender {

  private final Config config;
  private final PackingPlan plan;
  private final FormatterUtils formatter;

  public SubmitTableDryRunRenderer(SubmitDryRunResponse response, boolean rich) {
    this.config = response.getConfig();
    this.plan = response.getPackingPlan();
    this.formatter = new FormatterUtils(rich);
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
      String header = formatter.renderContainerName(containerId);
      containerBuilder.append(header + "\n");
      PackingPlan.ContainerPlan containerPlan = containersMap.get(containerId);
      containerBuilder.append(formatter.renderResourceUsage(
          containerPlan.getRequiredResource()) + "\n");
      List<FormatterUtils.Row> rows = new ArrayList<>();
      for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
        rows.add(formatter.rowOfInstancePlan(instancePlan,
            FormatterUtils.TextColor.DEFAULT, FormatterUtils.TextStyle.DEFAULT));
      }
      containerBuilder.append(formatter.renderOneContainer(rows));
      containerTables.add(containerBuilder.toString());
    }
    builder.append(String.join("\n", containerTables));
    return builder.toString();
  }
}
