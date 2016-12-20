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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;

import com.twitter.heron.spi.packing.Resource;

/**
 * Table dry-run render
 */
public class TableDryRunRender extends DryRunRender {

  private List<String> title = Arrays.asList(
      "component", "cpu", "disk (GB)", "ram (GB)", "parallelism");

  private StringBuilder addRow(StringBuilder builder, String row) {
    builder.append(row);
    builder.append('\n');
    return builder;
  }

  /**
   * generate formatter for each row based on rows. Width of a column is the
   * max width of all cells on that column
   *
   * @param rows Each row in table
   * @return formatter for row
   */
  private String generateRowFormatter(List<List<String>> rows) {
    Integer[] width = new Integer[title.size()];
    for (int i = 0; i < title.size(); i++) {
      width[i] = title.get(i).length();
    }
    for (List<String> row: rows) {
      for (int i = 0; i < row.size(); i++) {
        width[i] = Math.max(width[i], row.get(i).length());
      }
    }
    StringBuilder metaFormatterBuilder = new StringBuilder();
    String metaCellFormatter = "%%%ds";
    metaFormatterBuilder.append(Strings.repeat(String.format("| %s ", metaCellFormatter),
        title.size()));
    metaFormatterBuilder.append("|");
    return String.format(metaFormatterBuilder.toString(), (Object[]) width);
  }

  /**
   * Seal rows to create table
   * @param rows Each row in table
   * @return Formatted table
   */
  private String createTable(List<List<String>> rows) {
    String rowFormatter = generateRowFormatter(rows);
    String titleRow = String.format(
        rowFormatter, (Object[]) title.toArray(new String[title.size()]));
    StringBuilder builder = new StringBuilder();
    addRow(builder, Strings.repeat("=", titleRow.length()));
    addRow(builder, titleRow);
    addRow(builder, Strings.repeat("-", titleRow.length()));
    for (List<String> row: rows) {
      addRow(builder, String.format(rowFormatter, (Object[]) row.toArray(new String[row.size()])));
    }
    addRow(builder, Strings.repeat("=", titleRow.length()));
    return builder.toString();
  }

  @Override
  public String render(SubmitDryRunResponse resp) {
    Map<String, Resource> componentsResource =
        this.componentsResource(resp.getPackingPlan());
    Map<String, Integer> componentsParallelism =
        this.componentsParallelism(resp.getPackingPlan());
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

  /**
   * Format amount associated with percentage change
   * @param oldAmount old amount
   * @param newAmount new amount
   * @return formatted change
   */
  private String formatChange(long oldAmount, long newAmount) {
    long delta = newAmount - oldAmount;
    double percentage = (double) delta / (double) oldAmount;
    if (percentage == 0.0) {
      return String.valueOf(newAmount);
    } else {
      String sign = "";
      if (percentage > 0.0) {
        sign = "+";
      }
      return String.format("%d (%s%.2f%%)", newAmount, sign, percentage * 100.0);
    }
  }

  @Override
  public String render(UpdateDryRunResponse resp) {
    Map<String, Resource> newComponentsResource =
        componentsResource(resp.getPackingPlan());
    Map<String, Resource> oldComponentsResource =
        componentsResource(resp.getOldPackingPlan());
    Map<String, Integer> newComponentsContainersNum =
        componentsParallelism(resp.getPackingPlan());
    Map<String, Integer> oldComponentsContainersNum =
        componentsParallelism(resp.getOldPackingPlan());
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
}
