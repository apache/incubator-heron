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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;

public class UpdateDryRunRender implements DryRunRender {

  private final UpdateDryRunResponse response;
  private final PackingPlan oldPlan;
  private final PackingPlan newPlan;

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

  private enum ContainerChange {
    UNAFFECTED,
    MODIFIED,
    NEW,
    REMOVED
  }

  public UpdateDryRunRender(UpdateDryRunResponse response) {
    this.response = response;
    this.oldPlan = response.getOldPackingPlan();
    this.newPlan = response.getPackingPlan();
  }

  private class TableRenderer {

    private final PackingPlan oldPlan;
    private final PackingPlan newPlan;

    TableRenderer(PackingPlan oldPlan, PackingPlan newPlan) {
      this.oldPlan = oldPlan;
      this.newPlan = newPlan;
    }

    private String renderContainerDiffView(int containerId, ContainersDiffView diffView) {
      StringBuilder builder = new StringBuilder();
      Optional<PackingPlan.ContainerPlan> oldPackingPlan = diffView.getOldPlan();
      Optional<PackingPlan.ContainerPlan> newPackingPlan = diffView.getNewPlan();
      String header = new FormatterUtils.Cell(
          String.format("Container %d: ", containerId), FormatterUtils.TextStyle.BOLD).toString();
      builder.append(header);
      // Container exists in both old and new packing plan
      if (oldPackingPlan.isPresent() && newPackingPlan.isPresent()) {
        PackingPlan.ContainerPlan newContainerPlan = newPackingPlan.get();
        PackingPlan.ContainerPlan oldContainerPlan = oldPackingPlan.get();
        // Container plan did not change
        if (newContainerPlan.equals(oldContainerPlan)) {
          builder.append(ContainerChange.UNAFFECTED + "\n");
          String resourceUsage = FormatterUtils.renderResourceUsage(
              newContainerPlan.getRequiredResource());
          List<FormatterUtils.Row> rows = new ArrayList<>();
          for (PackingPlan.InstancePlan plan: newContainerPlan.getInstances()) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan,
                FormatterUtils.TextColor.DEFAULT,
                FormatterUtils.TextStyle.DEFAULT));
          }
          String containerTable = FormatterUtils.renderOneContainer(rows);
          builder.append(resourceUsage + "\n");
          builder.append(containerTable + "\n");
        } else {
          // Container plan has changed
          String resourceUsage = FormatterUtils.renderResourceUsageChange(
              oldContainerPlan.getRequiredResource(), newContainerPlan.getRequiredResource());
          Set<PackingPlan.InstancePlan> oldInstancePlans = oldContainerPlan.getInstances();
          Set<PackingPlan.InstancePlan> newInstancePlans = newContainerPlan.getInstances();
          Set<PackingPlan.InstancePlan> unchangedPlans =
              Sets.intersection(oldInstancePlans, newInstancePlans).immutableCopy();
          Set<PackingPlan.InstancePlan> newPlans =
              Sets.difference(newInstancePlans, oldInstancePlans);
          Set<PackingPlan.InstancePlan> removedPlans =
              Sets.difference(oldInstancePlans, newInstancePlans);
          List<FormatterUtils.Row> rows = new ArrayList<>();
          for (PackingPlan.InstancePlan plan: unchangedPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan,
                FormatterUtils.TextColor.DEFAULT,
                FormatterUtils.TextStyle.DEFAULT));
          }
          for (PackingPlan.InstancePlan plan: newPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan,
                FormatterUtils.TextColor.GREEN,
                FormatterUtils.TextStyle.DEFAULT));
          }
          for (PackingPlan.InstancePlan plan: removedPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(
                plan, FormatterUtils.TextColor.RED, FormatterUtils.TextStyle.STRIKETHROUGH));
          }
          builder.append(new FormatterUtils.Cell(
              ContainerChange.MODIFIED.toString()).toString() + "\n");
          builder.append(resourceUsage + "\n");
          String containerTable = FormatterUtils.renderOneContainer(rows);
          builder.append(containerTable + "\n");
        }
      } else if (oldPackingPlan.isPresent()) {
        // Container has been removed
        PackingPlan.ContainerPlan oldContainerPlan = oldPackingPlan.get();
        List<FormatterUtils.Row> rows = new ArrayList<>();
        for (PackingPlan.InstancePlan plan: oldContainerPlan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(
              plan, FormatterUtils.TextColor.RED, FormatterUtils.TextStyle.STRIKETHROUGH));
        }
        builder.append(new FormatterUtils.Cell(ContainerChange.REMOVED.toString(),
            FormatterUtils.TextColor.RED).toString() + "\n");
        builder.append(FormatterUtils.renderResourceUsage(
            oldContainerPlan.getRequiredResource()) + "\n");
        builder.append(FormatterUtils.renderOneContainer(rows) + "\n");
      } else if (newPackingPlan.isPresent()) {
        // New container has been added
        PackingPlan.ContainerPlan newContainerPlan = newPackingPlan.get();
        List<FormatterUtils.Row> rows = new ArrayList<>();
        for (PackingPlan.InstancePlan plan: newContainerPlan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(plan,
              FormatterUtils.TextColor.GREEN,
              FormatterUtils.TextStyle.DEFAULT));
        }
        builder.append(new FormatterUtils.Cell(ContainerChange.NEW.toString(),
            FormatterUtils.TextColor.GREEN).toString() + "\n");
        builder.append(FormatterUtils.renderResourceUsage(
            newContainerPlan.getRequiredResource()) + "\n");
        builder.append(FormatterUtils.renderOneContainer(rows) + "\n");
      } else {
        throw new RuntimeException(
            "Unexpected error: either new container plan or old container plan has to exist");
      }
      return builder.toString();
    }

    public String render() {
      Map<Integer, ContainersDiffView> diffViews = getContainerDiffViews(oldPlan, newPlan);
      StringBuilder builder = new StringBuilder();
      for (Integer containerId: diffViews.keySet()) {
        ContainersDiffView view = diffViews.get(containerId);
        builder.append(renderContainerDiffView(containerId, view));
      }
      return builder.toString();
    }
  }

  public String renderTable() {
    return new TableRenderer(oldPlan, newPlan).render();
  }

  public String renderRaw() {
    StringBuilder builder = new StringBuilder();
    String topologyName = response.getTopology().getName();
    String packingClassName = Context.packingClass(response.getConfig());
    builder.append("Packing class: " + packingClassName + "\n");
    builder.append("Topology: " + topologyName + "\n");
    builder.append("New packing plan:\n");
    builder.append(response.getPackingPlan().toString() + "\n");
    builder.append("Old packing plan:\n");
    builder.append(response.getOldPackingPlan().toString() + "\n");
    return builder.toString();
  }
}
