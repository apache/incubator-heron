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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.*;
import com.twitter.heron.scheduler.dryrun.FormatterUtils.*;

public class UpdateDryRunRender implements DryRunRender {

  private final UpdateDryRunResponse response;
  private final PackingPlan oldPlan;
  private final PackingPlan newPlan;

  private class ContainersDiffView {
    private final Optional<ContainerPlan> oldPlan;
    private final Optional<ContainerPlan> newPlan;

    public ContainersDiffView(Optional<ContainerPlan> oldPlan,
                              Optional<ContainerPlan> newPlan) {
      this.oldPlan = oldPlan;
      this.newPlan = newPlan;
    }

    public Optional<ContainerPlan> getOldPlan() {
      return oldPlan;
    }

    public Optional<ContainerPlan> getNewPlan() {
      return newPlan;
    }
  }

  private Map<Integer, ContainersDiffView> getContainerDiffViews(PackingPlan oldPlan,
                                                                 PackingPlan newPlan) {
    Map<Integer, ContainersDiffView> diffView = new HashMap<>();
    for(PackingPlan.ContainerPlan plan: oldPlan.getContainers()) {
      int id = plan.getId();
      diffView.put(id, new ContainersDiffView(oldPlan.getContainer(id), newPlan.getContainer(id)));
    }
    for(PackingPlan.ContainerPlan plan: newPlan.getContainers()) {
      int id = plan.getId();
      if(!diffView.containsKey(id)) {
        diffView.put(id, new ContainersDiffView(oldPlan.getContainer(id),
            newPlan.getContainer(id)));
      }
    }
    return diffView;
  }

  private enum ContainerChange {
    UNAFFECTED,
    MODIFIED,
    NEW,
    REMOVED;
  }

  public UpdateDryRunRender(UpdateDryRunResponse response) {
    this.response = response;
    this.oldPlan = response.getOldPackingPlan();
    this.newPlan = response.getPackingPlan();
  }

  private class TableRenderer {

    private final PackingPlan oldPlan;
    private final PackingPlan newPlan;

    public TableRenderer(PackingPlan oldPlan, PackingPlan newPlan) {
      this.oldPlan = oldPlan;
      this.newPlan = newPlan;
    }

    private String renderContainerDiffView(int containerId, ContainersDiffView diffView) {
      StringBuilder builder = new StringBuilder();
      Optional<ContainerPlan> oldPlan = diffView.getOldPlan();
      Optional<ContainerPlan> newPlan = diffView.getNewPlan();
      String header = new Cell(
        String.format("Container %d: ", containerId), TextStyle.BOLD).toString();
      builder.append(header);
      // Container exists in both old and new packing plan
      if(oldPlan.isPresent() && newPlan.isPresent()) {
        ContainerPlan newContainerPlan = newPlan.get();
        ContainerPlan oldContainerPlan = oldPlan.get();
        // Container plan did not change
        if (newContainerPlan.equals(oldContainerPlan)) {
          builder.append(ContainerChange.UNAFFECTED + "\n");
          String resourceUsage = FormatterUtils.renderResourceUsage(
              newContainerPlan.getRequiredResource());
          List<Row> rows = new ArrayList<>();
          for(InstancePlan plan: newContainerPlan.getInstances()) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.DEFAULT, TextStyle.DEFAULT));
          }
          String containerTable = FormatterUtils.renderOneContainer(rows);
          builder.append(resourceUsage + "\n");
          builder.append(containerTable + "\n");
        } else {
          // Container plan has changed
          String resourceUsage = FormatterUtils.renderResourceUsageChange(
              oldContainerPlan.getRequiredResource(), newContainerPlan.getRequiredResource());
          Set<InstancePlan> oldInstancePlans = oldContainerPlan.getInstances();
          Set<InstancePlan> newInstancePlans = newContainerPlan.getInstances();
          Set<InstancePlan> unchangedPlans =
              Sets.intersection(oldInstancePlans, newInstancePlans).immutableCopy();
          Set<InstancePlan> newPlans =
              Sets.difference(newInstancePlans, oldInstancePlans);
          Set<InstancePlan> removedPlans =
              Sets.difference(oldInstancePlans, newInstancePlans);
          List<Row> rows = new ArrayList<>();
          for(InstancePlan plan: unchangedPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.DEFAULT, TextStyle.DEFAULT));
          }
          for(InstancePlan plan: newPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.GREEN, TextStyle.DEFAULT));
          }
          for(InstancePlan plan: removedPlans) {
            rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.RED, TextStyle.STRIKETHROUGH));
          }
          builder.append(new Cell(ContainerChange.MODIFIED.toString()).toString() + "\n");
          builder.append(resourceUsage + "\n");
          String containerTable = FormatterUtils.renderOneContainer(rows);
          builder.append(containerTable + "\n");
        }
      } else if (oldPlan.isPresent()) {
        // Container has been removed
        ContainerPlan oldContainerPlan = oldPlan.get();
        List<Row> rows = new ArrayList<>();
        for(InstancePlan plan: oldContainerPlan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.RED, TextStyle.STRIKETHROUGH));
        }
        builder.append(new Cell(ContainerChange.REMOVED.toString(), TextColor.RED).toString() + "\n");
        builder.append(FormatterUtils.renderResourceUsage(
            oldContainerPlan.getRequiredResource()) + "\n");
        builder.append(FormatterUtils.renderOneContainer(rows) + "\n");
      } else if (newPlan.isPresent()) {
        // New container has been added
        ContainerPlan newContainerPlan = newPlan.get();
        List<Row> rows = new ArrayList<>();
        for(InstancePlan plan: newContainerPlan.getInstances()) {
          rows.add(FormatterUtils.rowOfInstancePlan(plan, TextColor.GREEN, TextStyle.DEFAULT));
        }
        builder.append(new Cell(ContainerChange.NEW.toString(), TextColor.GREEN).toString() + "\n");
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
      for(Integer containerId: diffViews.keySet()) {
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
