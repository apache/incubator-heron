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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.PackingPlan;

import static org.apache.heron.scheduler.dryrun.FormatterUtils.ContainerChange;
import static org.apache.heron.scheduler.dryrun.FormatterUtils.Row;
import static org.apache.heron.scheduler.dryrun.FormatterUtils.TextColor;
import static org.apache.heron.scheduler.dryrun.FormatterUtils.TextStyle;

/**
 * Dry-run renderer that renders update dry-run response in table format
 */
public class UpdateTableDryRunRenderer implements DryRunRender {

  private class ContainersDiffView {
    private final Optional<PackingPlan.ContainerPlan> oldPlan;
    private final Optional<PackingPlan.ContainerPlan> newPlan;

    ContainersDiffView(Optional<PackingPlan.ContainerPlan> oldPlan,
                              Optional<PackingPlan.ContainerPlan> newPlan) {
      this.oldPlan = oldPlan;
      this.newPlan = newPlan;
    }

    public Optional<PackingPlan.ContainerPlan> getOldPlan() {
      return oldPlan;
    }

    public Optional<PackingPlan.ContainerPlan> getNewPlan() {
      return newPlan;
    }
  }

  private Map<Integer, ContainersDiffView> getContainerDiffViews(PackingPlan oldPackingPlan,
                                                                 PackingPlan newPackingPlan) {
    Map<Integer, ContainersDiffView> diffView = new HashMap<>();
    for (PackingPlan.ContainerPlan plan: oldPackingPlan.getContainers()) {
      int id = plan.getId();
      diffView.put(id, new ContainersDiffView(oldPackingPlan.getContainer(id),
          newPackingPlan.getContainer(id)));
    }
    for (PackingPlan.ContainerPlan plan: newPackingPlan.getContainers()) {
      int id = plan.getId();
      if (!diffView.containsKey(id)) {
        diffView.put(id, new ContainersDiffView(oldPackingPlan.getContainer(id),
            newPackingPlan.getContainer(id)));
      }
    }
    return diffView;
  }

  private final Config config;
  private final PackingPlan oldPlan;
  private final PackingPlan newPlan;
  private final FormatterUtils formatter;

  public UpdateTableDryRunRenderer(UpdateDryRunResponse response, boolean rich) {
    this.config = response.getConfig();
    this.oldPlan = response.getOldPackingPlan();
    this.newPlan = response.getPackingPlan();
    this.formatter = new FormatterUtils(rich);
  }

  private String renderContainerDiffView(int containerId, ContainersDiffView diffView) {
    StringBuilder builder = new StringBuilder();
    Optional<PackingPlan.ContainerPlan> oldPackingPlan = diffView.getOldPlan();
    Optional<PackingPlan.ContainerPlan> newPackingPlan = diffView.getNewPlan();
    String header = String.format("%s ", formatter.renderContainerName(containerId));
    builder.append(header);
    // Container exists in both old and new packing plan
    if (oldPackingPlan.isPresent() && newPackingPlan.isPresent()) {
      PackingPlan.ContainerPlan newContainerPlan = newPackingPlan.get();
      PackingPlan.ContainerPlan oldContainerPlan = oldPackingPlan.get();
      // Container plan did not change
      if (newContainerPlan.equals(oldContainerPlan)) {
        builder.append(formatter.renderContainerChange(ContainerChange.UNAFFECTED) + "\n");
        String resourceUsage = formatter.renderResourceUsage(
            newContainerPlan.getRequiredResource());
        List<Row> rows = new ArrayList<>();
        for (PackingPlan.InstancePlan plan: newContainerPlan.getInstances()) {
          rows.add(formatter.rowOfInstancePlan(plan,
              TextColor.DEFAULT,
              TextStyle.DEFAULT));
        }
        String containerTable = formatter.renderOneContainer(rows);
        builder.append(resourceUsage + "\n");
        builder.append(containerTable);
      } else {
        // Container plan has changed
        String resourceUsage = formatter.renderResourceUsageChange(
            oldContainerPlan.getRequiredResource(), newContainerPlan.getRequiredResource());
        Set<PackingPlan.InstancePlan> oldInstancePlans = oldContainerPlan.getInstances();
        Set<PackingPlan.InstancePlan> newInstancePlans = newContainerPlan.getInstances();
        Set<PackingPlan.InstancePlan> unchangedPlans =
            Sets.intersection(oldInstancePlans, newInstancePlans).immutableCopy();
        Set<PackingPlan.InstancePlan> newPlans =
            Sets.difference(newInstancePlans, oldInstancePlans);
        Set<PackingPlan.InstancePlan> removedPlans =
            Sets.difference(oldInstancePlans, newInstancePlans);
        List<Row> rows = new ArrayList<>();
        for (PackingPlan.InstancePlan plan: unchangedPlans) {
          rows.add(formatter.rowOfInstancePlan(plan,
              TextColor.DEFAULT,
              TextStyle.DEFAULT));
        }
        for (PackingPlan.InstancePlan plan: newPlans) {
          rows.add(formatter.rowOfInstancePlan(plan,
              TextColor.GREEN,
              TextStyle.DEFAULT));
        }
        for (PackingPlan.InstancePlan plan: removedPlans) {
          rows.add(formatter.rowOfInstancePlan(
              plan, TextColor.RED, TextStyle.STRIKETHROUGH));
        }
        builder.append(formatter.renderContainerChange(ContainerChange.MODIFIED) + "\n");
        builder.append(resourceUsage + "\n");
        String containerTable = formatter.renderOneContainer(rows);
        builder.append(containerTable);
      }
    } else if (oldPackingPlan.isPresent()) {
      // Container has been removed
      PackingPlan.ContainerPlan oldContainerPlan = oldPackingPlan.get();
      List<Row> rows = new ArrayList<>();
      for (PackingPlan.InstancePlan plan: oldContainerPlan.getInstances()) {
        rows.add(formatter.rowOfInstancePlan(
            plan, TextColor.RED, TextStyle.STRIKETHROUGH));
      }
      builder.append(formatter.renderContainerChange(ContainerChange.REMOVED) + "\n");
      builder.append(formatter.renderResourceUsage(
          oldContainerPlan.getRequiredResource()) + "\n");
      builder.append(formatter.renderOneContainer(rows));
    } else if (newPackingPlan.isPresent()) {
      // New container has been added
      PackingPlan.ContainerPlan newContainerPlan = newPackingPlan.get();
      List<Row> rows = new ArrayList<>();
      for (PackingPlan.InstancePlan plan: newContainerPlan.getInstances()) {
        rows.add(formatter.rowOfInstancePlan(plan,
            TextColor.GREEN,
            TextStyle.DEFAULT));
      }
      builder.append(formatter.renderContainerChange(ContainerChange.NEW) + "\n");
      builder.append(formatter.renderResourceUsage(
          newContainerPlan.getRequiredResource()) + "\n");
      builder.append(formatter.renderOneContainer(rows));
    } else {
      throw new RuntimeException(
          "Unexpected error: either new container plan or old container plan has to exist");
    }
    return builder.toString();
  }

  public String render() {
    Map<Integer, ContainersDiffView> diffViews = getContainerDiffViews(oldPlan, newPlan);
    int numContainers = newPlan.getContainers().size();
    StringBuilder builder = new StringBuilder();
    builder.append(String.format("Total number of containers: %d", numContainers) + "\n");
    builder.append(String.format("Using repacking class: %s",
        Context.repackingClass(config)) + "\n");
    List<String> containerTables = new ArrayList<>();
    for (Integer id: diffViews.keySet()) {
      containerTables.add(renderContainerDiffView(id, diffViews.get(id)));
    }
    builder.append(String.join("\n", containerTables));
    return builder.toString();
  }
}
