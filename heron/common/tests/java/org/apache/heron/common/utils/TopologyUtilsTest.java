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

package org.apache.heron.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.utils.topology.TopologyTests;

public class TopologyUtilsTest {
  @Test
  public void testGetComponentParallelism() {
    int componentParallelism = 4;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology =
        TopologyTests.createTopology("testTopology", topologyConfig, spouts, bolts);
    Map<String, Integer> componentParallelismMap =
        TopologyUtils.getComponentParallelism(topology);
    Assert.assertEquals(componentParallelism, componentParallelismMap.get("spout").intValue());
    Assert.assertEquals(componentParallelism, componentParallelismMap.get("bolt").intValue());
  }

  @Test
  public void testGetTotalInstance() {
    int componentParallelism = 4;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology =
        TopologyTests.createTopology("testTopology", topologyConfig, spouts, bolts);
    Assert.assertEquals((spouts.size() + bolts.size()) * componentParallelism,
        TopologyUtils.getTotalInstance(topology));
  }

  @Test
  public void testGetComponentCpuMapDefaultValue() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);

    // sort the component CPU map
    Map<String, Double> cpuMap = new TreeMap<>(TopologyUtils.getComponentCpuMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component CPU map is not set, the cpuMap size should be 0
    Assert.assertEquals(0, cpuMap.size());
  }

  @Test
  public void testGetComponentCpuMapAllCpuSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    double boltCpu = 1.0;
    double spoutCpu = 2.0;
    topologyConfig.setComponentCpu("spout", spoutCpu);
    topologyConfig.setComponentCpu("bolt", boltCpu);

    // sort the component CPU map
    Map<String, Double> cpuMap = new TreeMap<>(TopologyUtils.getComponentCpuMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, cpuMap.keySet().toArray());
    Assert.assertArrayEquals(new Double[]{boltCpu, spoutCpu}, cpuMap.values().toArray());
  }

  @Test
  public void testGetComponentCpuMapSomeCpuSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    double spoutCpu = 2.0f;
    topologyConfig.setComponentCpu("spout", spoutCpu);

    // sort the component CPU map
    Map<String, Double> cpuMap = new TreeMap<>(TopologyUtils.getComponentCpuMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component CPU map sets only spout's CPU
    Assert.assertArrayEquals(new String[]{"spout"}, cpuMap.keySet().toArray());
    Assert.assertArrayEquals(new Double[]{spoutCpu}, cpuMap.values().toArray());
  }

  @Test
  public void testGetComponentRamMapDefaultValue() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);

    // sort the component RAM map
    Map<String, ByteAmount> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component RAM map is not set, the ramMap size should be 0
    Assert.assertEquals(0, ramMap.size());
  }

  @Test
  public void testGetComponentRamMapAllRamSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    ByteAmount boltRam = ByteAmount.fromGigabytes(1);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);
    topologyConfig.setComponentRam("spout", spoutRam);
    topologyConfig.setComponentRam("bolt", boltRam);

    // sort the component RAM map
    Map<String, ByteAmount> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, ramMap.keySet().toArray());
    Assert.assertArrayEquals(new ByteAmount[]{boltRam, spoutRam}, ramMap.values().toArray());
  }

  @Test
  public void testGetComponentRamMapSomeRamSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    ByteAmount spoutRam = ByteAmount.fromGigabytes(2);
    topologyConfig.setComponentRam("spout", spoutRam);

    // sort the component RAM map
    Map<String, ByteAmount> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component RAM map sets only spout's RAM
    Assert.assertArrayEquals(new String[]{"spout"}, ramMap.keySet().toArray());
    Assert.assertArrayEquals(new ByteAmount[]{spoutRam}, ramMap.values().toArray());
  }

  @Test
  public void testGetComponentDiskMapDefaultValue() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);

    // sort the component disk map
    Map<String, ByteAmount> diskMap = new TreeMap<>(TopologyUtils.getComponentDiskMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component disk map is not set, the diskMap size should be 0
    Assert.assertEquals(0, diskMap.size());
  }

  @Test
  public void testGetComponentDiskMapAllDiskSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    ByteAmount boltDisk = ByteAmount.fromGigabytes(1);
    ByteAmount spoutDisk = ByteAmount.fromGigabytes(2);
    topologyConfig.setComponentDisk("spout", spoutDisk);
    topologyConfig.setComponentDisk("bolt", boltDisk);

    // sort the component disk map
    Map<String, ByteAmount> diskMap = new TreeMap<>(TopologyUtils.getComponentDiskMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, diskMap.keySet().toArray());
    Assert.assertArrayEquals(new ByteAmount[]{boltDisk, spoutDisk}, diskMap.values().toArray());
  }

  @Test
  public void testGetComponentDiskMapSomeDiskSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    ByteAmount spoutDisk = ByteAmount.fromGigabytes(2);
    topologyConfig.setComponentDisk("spout", spoutDisk);

    // sort the component disk map
    Map<String, ByteAmount> diskMap = new TreeMap<>(TopologyUtils.getComponentDiskMapConfig(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts)));
    // Component disk map sets only spout's disk
    Assert.assertArrayEquals(new String[]{"spout"}, diskMap.keySet().toArray());
    Assert.assertArrayEquals(new ByteAmount[]{spoutDisk}, diskMap.values().toArray());
  }

  @Test
  public void testBadTopologyName() {
    int componentParallelism = 2;
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    Assert.assertFalse(TopologyUtils.verifyTopology(TopologyTests.createTopology(
        "test.topology" /* Bad topology name */, new Config(), spouts, bolts)));
  }

  @Test
  public void testValidTopology() {
    int componentParallelism = 2;
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    Map<String, String> connections = new HashMap<>();
    connections.put("bolt", "spout");
    Assert.assertTrue(TopologyUtils.verifyTopology(TopologyTests.createTopologyWithConnection(
        "testTopology"  /* Bad topology name */, new Config(), spouts, bolts, connections)));
  }
}
