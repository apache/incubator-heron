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

package com.twitter.heron.spi.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;

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
  public void testGetComponentRamMapDefaultValue() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    long defaultValue = 1 * Constants.GB;

    // sort the component ram map
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMap(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts), defaultValue));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, ramMap.keySet().toArray());
    Assert.assertArrayEquals(new Long[]{defaultValue, defaultValue}, ramMap.values().toArray());
  }

  @Test
  public void testGetComponentRamMapAllRamSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    long boltRam = 1 * Constants.GB;
    long spoutRam = 2 * Constants.GB;
    topologyConfig.setComponentRam("spout", spoutRam);
    topologyConfig.setComponentRam("bolt", boltRam);

    // sort the component ram map
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMap(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts), 0));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, ramMap.keySet().toArray());
    Assert.assertArrayEquals(new Long[]{boltRam, spoutRam}, ramMap.values().toArray());
  }

  @Test
  public void testGetComponentRamMapSomeRamSpecified() {
    int componentParallelism = 2;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    long defaultValue = 1 * Constants.GB;
    long spoutRam = 2 * Constants.GB;
    topologyConfig.setComponentRam("spout", spoutRam);

    // sort the component ram map
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtils.getComponentRamMap(
        TopologyTests.createTopology("test", topologyConfig, spouts, bolts), defaultValue));
    Assert.assertArrayEquals(new String[]{"bolt", "spout"}, ramMap.keySet().toArray());
    Assert.assertArrayEquals(new Long[]{defaultValue, spoutRam}, ramMap.values().toArray());

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
