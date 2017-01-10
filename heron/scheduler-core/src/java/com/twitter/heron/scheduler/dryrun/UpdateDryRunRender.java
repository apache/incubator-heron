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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.*;
import com.twitter.heron.spi.packing.Resource;
import com.twitter.heron.scheduler.dryrun.FormatterUtils.*;

public class UpdateDryRunRender extends DryRunRender {

  private final UpdateDryRunResponse response;
  private final Map<Integer, ContainersDiffView> diffViews;

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

  public UpdateDryRunRender(UpdateDryRunResponse response) {
    this.response = response;
    PackingPlan oldPlan = response.getOldPackingPlan();
    PackingPlan newPlan = response.getPackingPlan();
    this.diffViews = getContainerDiffViews(oldPlan, newPlan);
  }

  private Row rowOfInstancePlan(InstancePlan plan, TextColor color, TextStyle style) {
    String taskId = String.valueOf(plan.getTaskId());
    String cpu = String.valueOf(plan.getResource().getCpu());
    String ram = String.valueOf(plan.getResource().getRam().asGigabytes());
    String disk = String.valueOf(plan.getResource().getDisk().asGigabytes());
    List<String> cells = Arrays.asList(
          plan.getComponentName(), taskId, cpu, ram, disk);
    Row row = new Row(cells);
    row.setStyle(style);
    row.setColor(color);
    return row;
  }

  private String renderOneContainer(List<Row> rows) {
    List<String> titleNames = Arrays.asList(
        "component", "task ID", "CPU", "RAM (GB)", "disk (GB)");
    Row title = new Row(titleNames);
    title.setStyle(TextStyle.BOLD);
    return new Table(title, rows).createTable();
  }

  private enum ContainerChange {
    UNAFFECTED,
    ENLARGED,
    REDUCED,
    MODIFIED,
    REMOVED,
    NEW;
  }

  private String renderResourceUsage(Resource resource) {
    double cpu = resource.getCpu();
    ByteAmount ram = resource.getRam();
    ByteAmount disk = resource.getDisk();
    return String.format("CPU: %s, RAM: %sGB, Disk: %sGB",
        cpu, ram.asGigabytes(), disk.asGigabytes());
  }

  private String renderResourceUsageChange(Resource oldResource, Resource newResource) {
    double oldCpu = oldResource.getCpu();
    double newCpu = newResource.getCpu();
    Optional<Cell> cpuUsageChange = FormatterUtils.percentageChange(oldCpu, newCpu);
    long oldRam = oldResource.getRam().asGigabytes();
    long newRam = newResource.getRam().asGigabytes();
    Optional<Cell> ramUsageChange = FormatterUtils.percentageChange(oldRam, newRam);
    long oldDisk = oldResource.getDisk().asGigabytes();
    long newDisk = newResource.getDisk().asGigabytes();
    Optional<Cell> diskUsageChange = FormatterUtils.percentageChange(oldDisk, newDisk);
    String cpuUsage = String.format("CPU: %s", newCpu);
    if (cpuUsageChange.isPresent()) {
      cpuUsage += String.format(" (%s)", cpuUsageChange.get().toString());
    }
    String ramUsage = String.format("RAM: %sGB", newRam);
    if (ramUsageChange.isPresent()) {
      ramUsage += String.format(" (%s)", ramUsageChange.get().toString());
    }
    String diskUsage = String.format("Disk: %sGB", newDisk);
    if (diskUsageChange.isPresent()) {
      diskUsage += String.format(" (%s)", diskUsageChange.get().toString());
    }
    return String.join(", ", cpuUsage, ramUsage, diskUsage);
  }

  private String renderOneContainerTable(int containerId, ContainersDiffView diffView) {
    StringBuilder builder = new StringBuilder();
    Optional<ContainerPlan> oldPlan = diffView.getOldPlan();
    Optional<ContainerPlan> newPlan = diffView.getNewPlan();
    String header = new Cell(
      String.format("Container %s: ", containerId), TextStyle.BOLD).toString();
    builder.append(header);
    // Container exists in both old and new packing plan
    if(oldPlan.isPresent() && newPlan.isPresent()) {
      ContainerPlan newContainerPlan = newPlan.get();
      ContainerPlan oldContainerPlan = oldPlan.get();
      // Container plan did not change
      if (newContainerPlan.equals(oldContainerPlan)) {
        builder.append(ContainerChange.UNAFFECTED + "\n");
        String resourceUsage = renderResourceUsage(newContainerPlan.getRequiredResource());
        List<Row> rows = new ArrayList<>();
        for(InstancePlan plan: newContainerPlan.getInstances()) {
          rows.add(rowOfInstancePlan(plan, TextColor.DEFAULT, TextStyle.DEFAULT));
        }
        String containerTable = renderOneContainer(rows);
        builder.append(resourceUsage + "\n");
        builder.append(containerTable + "\n");
      } else {
        // Container plan has changed
        String resourceUsage = renderResourceUsageChange(
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
          rows.add(rowOfInstancePlan(plan, TextColor.DEFAULT, TextStyle.DEFAULT));
        }
        for(InstancePlan plan: newPlans) {
          rows.add(rowOfInstancePlan(plan, TextColor.GREEN, TextStyle.DEFAULT));
        }
        for(InstancePlan plan: removedPlans) {
          rows.add(rowOfInstancePlan(plan, TextColor.RED, TextStyle.STRIKETHROUGH));
        }
        builder.append(new Cell(ContainerChange.MODIFIED.toString()).toString() + "\n");
        builder.append(resourceUsage + "\n");
        String containerTable = renderOneContainer(rows);
        builder.append(containerTable + "\n");
      }
    } else if (oldPlan.isPresent()) {
      // Container has been removed
      ContainerPlan oldContainerPlan = oldPlan.get();
      List<Row> rows = new ArrayList<>();
      for(InstancePlan plan: oldContainerPlan.getInstances()) {
        rows.add(rowOfInstancePlan(plan, TextColor.RED, TextStyle.STRIKETHROUGH));
      }
      builder.append(new Cell(ContainerChange.REMOVED.toString(), TextColor.RED).toString() + "\n");
      builder.append(renderResourceUsage(oldContainerPlan.getRequiredResource()) + "\n");
      builder.append(renderOneContainer(rows) + "\n");
    } else if (newPlan.isPresent()) {
      // New container has been added
      ContainerPlan newContainerPlan = newPlan.get();
      List<Row> rows = new ArrayList<>();
      for(InstancePlan plan: newContainerPlan.getInstances()) {
        rows.add(rowOfInstancePlan(plan, TextColor.GREEN, TextStyle.DEFAULT));
      }
      builder.append(new Cell(ContainerChange.NEW.toString(), TextColor.GREEN).toString() + "\n");
      builder.append(renderResourceUsage(newContainerPlan.getRequiredResource()) + "\n");
      builder.append(renderOneContainer(rows) + "\n");
    } else {
      throw new RuntimeException(
          "Unexpected error: either new container plan or old container plan has to exist");
    }
    return builder.toString();
  }

  public String renderTable() {
    StringBuilder builder = new StringBuilder();
    for(Integer containerId: diffViews.keySet()) {
      ContainersDiffView view = diffViews.get(containerId);
      builder.append(renderOneContainerTable(containerId, view));
    }
    return builder.toString();
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
