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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.packing.Resource;

public class JsonFormatterUtils {

  private final ObjectMapper mapper;

  public JsonFormatterUtils() {
    mapper = new ObjectMapper();
  }

  public String renderPackingPlan(String topologyName, String packingClass,
                                  PackingPlan packingPlan) throws JsonProcessingException {
    ObjectNode topLevel = mapper.createObjectNode();
    topLevel.put("packingClass", packingClass);
    topLevel.put("topology", topologyName);

    ArrayNode containers = mapper.createArrayNode();

    for (PackingPlan.ContainerPlan containerPlan : packingPlan.getContainers()) {
      ObjectNode planNode = mapper.createObjectNode();
      planNode.put("taskId", containerPlan.getId());
      containers.add(renderContainerPlan(containerPlan));
    }

    topLevel.set("containers", containers);

    return mapper.writeValueAsString(topLevel);
  }

  ObjectNode renderContainerPlan(PackingPlan.ContainerPlan containerPlan) {
    Resource requiredResources = containerPlan.getRequiredResource();
    ObjectNode resources = mapper.createObjectNode();
    resources.put("cpu", requiredResources.getCpu());
    resources.put("ram", requiredResources.getRam().asBytes());
    resources.put("disk", requiredResources.getDisk().asBytes());

    ObjectNode containerNode = mapper.createObjectNode();
    containerNode.put("id", containerPlan.getId());
    containerNode.set("resources", resources);

    ArrayNode components = mapper.createArrayNode();
    for (PackingPlan.InstancePlan instancePlan : containerPlan.getInstances()) {
      components.add(renderInstancePlan(instancePlan));
    }

    containerNode.set("components", components);

    return containerNode;
  }

  ObjectNode renderInstancePlan(PackingPlan.InstancePlan instancePlan) {
    Resource resources = instancePlan.getResource();
    ObjectNode resourcesNode = mapper.createObjectNode();
    resourcesNode.put("cpu", resources.getCpu());
    resourcesNode.put("ram", resources.getRam().asBytes());
    resourcesNode.put("disk", resources.getDisk().asBytes());

    ObjectNode instancePlanNode = mapper.createObjectNode();
    instancePlanNode.put("component", instancePlan.getComponentName());
    instancePlanNode.put("id", instancePlan.getTaskId());
    instancePlanNode.put("index", instancePlan.getComponentIndex());
    instancePlanNode.set("resources", resourcesNode);

    return instancePlanNode;
  }
}
