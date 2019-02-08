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

package org.apache.heron.packing;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.packing.utils.PackingUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.packing.IPacking;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.Resource;

import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_PADDING;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_DISK_REQUESTED;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_MAX_NUM_INSTANCES;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_PADDING_PERCENTAGE;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_PADDING;
import static org.apache.heron.api.Config.TOPOLOGY_CONTAINER_RAM_REQUESTED;

/**
 * Common configuration finalization for packing algorithms
 * Packing algorithms that extend this class should assume that:
 * <p>
 * 1. Instance default resources are read from:
 *  heron.resources.instance.ram,
 *  heron.resources.instance.cpu,
 *  heron.resources.instance.disk
 * <p>
 * 2. Padding resource percentage is read from:
 *  topology.container.padding.percentage
 *  or taken from PackingUtils.DEFAULT_CONTAINER_PADDING_PERCENTAGE
 * <p>
 * 3. Padding resource values are read from:
 *  topology.container.ram.padding or taken from PackingUtils.DEFAULT_CONTAINER_RAM_PADDING,
 *  topology.container.cpu.padding or taken from PackingUtils.DEFAULT_CONTAINER_CPU_PADDING
 * <p>
 * 4. Max number of instances per container is read from:
 *  topology.container.max.instances
 *  or taken from PackingUtils.DEFAULT_MAX_NUM_INSTANCES_PER_CONTAINER
 * <p>
 * 5. Container resources requirements are read from:
 *  topology.container.cpu or calculated from maxNumInstancesPerContainer * instanceDefaultCpu
 *  topology.container.ram or calculated from maxNumInstancesPerContainer * instanceDefaultRam
 *  topology.container.disk or calculated from maxNumInstancesPerContainer * instanceDefaultDisk
 * <p>
 * 6. Padding resource is finalized by:
 *  Math.max(containerResource * paddingPercentage, paddingValue)
 * <p>
 *
 * Subclasses that extend this class should just need to create PackingPlanBuilder with:
 *  defaultInstanceResource,
 *  maxContainerResource,
 *  containerPadding,
 *  componentResourceMap,
 *  instanceConstraints,
 *  and packingConstraints set.
 */
public abstract class AbstractPacking implements IPacking, IRepacking {
  private static final Logger LOG = Logger.getLogger(AbstractPacking.class.getName());
  protected TopologyAPI.Topology topology;

  // instance & container
  protected Resource defaultInstanceResources;
  protected Resource maxContainerResources;
  protected int maxNumInstancesPerContainer;
  protected Map<String, Resource> componentResourceMap;

  // padding
  protected Resource padding;

  @Override
  public void initialize(Config config, TopologyAPI.Topology inputTopology) {
    this.topology = inputTopology;
    setPackingConfigs(config);

    LOG.info(String.format("Initalizing Packing: \n"
            + "Max number of instances per container: %d \n"
            + "Default instance resource, CPU: %f, RAM: %s, DISK: %s \n"
            + "Paddng: %s \n"
            + "Container resource, CPU: %f, RAM: %s, DISK: %s",
        this.maxNumInstancesPerContainer,
        this.defaultInstanceResources.getCpu(),
        this.defaultInstanceResources.getRam().toString(),
        this.defaultInstanceResources.getDisk().toString(),
        this.padding.toString(),
        this.maxContainerResources.getCpu(),
        this.maxContainerResources.getRam().toString(),
        this.maxContainerResources.getDisk().toString()));
  }

  /**
   * Instatiate the packing algorithm parameters related to this topology.
   */
  private void setPackingConfigs(Config config) {
    List<TopologyAPI.Config.KeyValue> topologyConfig = topology.getTopologyConfig().getKvsList();

    // instance default resources are acquired from heron system level config
    this.defaultInstanceResources = new Resource(
        Context.instanceCpu(config),
        Context.instanceRam(config),
        Context.instanceDisk(config));

    int paddingPercentage = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_PADDING_PERCENTAGE, PackingUtils.DEFAULT_CONTAINER_PADDING_PERCENTAGE);
    ByteAmount ramPadding = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_RAM_PADDING, PackingUtils.DEFAULT_CONTAINER_RAM_PADDING);
    double cpuPadding = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_CPU_PADDING, PackingUtils.DEFAULT_CONTAINER_CPU_PADDING);
    Resource preliminaryPadding = new Resource(cpuPadding, ramPadding,
        PackingUtils.DEFAULT_CONTAINER_DISK_PADDING);

    this.maxNumInstancesPerContainer = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_MAX_NUM_INSTANCES, PackingUtils.DEFAULT_MAX_NUM_INSTANCES_PER_CONTAINER);

    // container default resources are computed as:
    // max number of instances per container * default instance resources
    double containerDefaultCpu = this.defaultInstanceResources.getCpu()
        * maxNumInstancesPerContainer;
    ByteAmount containerDefaultRam = this.defaultInstanceResources.getRam()
        .multiply(maxNumInstancesPerContainer);
    ByteAmount containerDefaultDisk = this.defaultInstanceResources.getDisk()
        .multiply(maxNumInstancesPerContainer);

    double containerCpu = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_CPU_REQUESTED, containerDefaultCpu);
    ByteAmount containerRam = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_RAM_REQUESTED, containerDefaultRam);
    ByteAmount containerDisk = TopologyUtils.getConfigWithDefault(topologyConfig,
        TOPOLOGY_CONTAINER_DISK_REQUESTED, containerDefaultDisk);
    Resource containerResource = new Resource(containerCpu,
        containerRam, containerDisk);

    // finalize padding
    this.padding = PackingUtils.finalizePadding(containerResource,
        preliminaryPadding, paddingPercentage);

    // finalize container resources
    this.maxContainerResources = containerResource;

    this.componentResourceMap = PackingUtils.getComponentResourceMap(
        TopologyUtils.getComponentParallelism(topology).keySet(),
        TopologyUtils.getComponentRamMapConfig(topology),
        TopologyUtils.getComponentCpuMapConfig(topology),
        TopologyUtils.getComponentDiskMapConfig(topology),
        defaultInstanceResources
    );
  }

  @Override
  public void close() {
  }
}
