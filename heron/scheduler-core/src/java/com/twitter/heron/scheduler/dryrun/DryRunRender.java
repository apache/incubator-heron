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
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Strings;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

/**
 * Interface of class that renders dry-run response
 */
public abstract class DryRunRender {

  private List<String> title = Arrays.asList(
      "component", "cpu", "disk (GB)", "ram (GB)", "parallelism");

  private StringBuilder addRow(StringBuilder builder, String row) {
    builder.append(row);
    builder.append('\n');
    return builder;
  }

  protected Map<String, Resource> componentsResource(PackingPlan packingPlan) {
    Map<String, Resource> componentsResource = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan: packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
        Resource resource = instancePlan.getResource();
        String componentName = instancePlan.getComponentName();
        Resource totalResource = componentsResource.get(componentName);
        if (totalResource == null) {
          componentsResource.put(componentName, resource);
        } else {
          componentsResource.replace(componentName, resource.plus(totalResource));
        }
      }
    }
    return componentsResource;
  }

  protected Map<String, Integer> componentsParallelism(PackingPlan packingPlan) {
    Map<String, Integer> componentsParallelism = new HashMap<>();
    for (PackingPlan.ContainerPlan containerPlan: packingPlan.getContainers()) {
      for (PackingPlan.InstancePlan instancePlan: containerPlan.getInstances()) {
        String componentName = instancePlan.getComponentName();
        Integer parallelism = componentsParallelism.get(componentName);
        if (parallelism == null) {
          componentsParallelism.put(componentName, 1);
        } else {
          componentsParallelism.replace(componentName, 1 + parallelism);
        }
      }
    }
    return componentsParallelism;
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

  public abstract String renderTable();
  public abstract String renderRaw();

}
