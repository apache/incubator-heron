package com.twitter.heron.scheduler.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronSubmitter;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.bolt.BaseBasicBolt;
import com.twitter.heron.api.bolt.BasicOutputCollector;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.spout.BaseRichSpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.BoltDeclarer;
import com.twitter.heron.api.topology.OutputFieldsDeclarer;
import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.api.topology.TopologyContext;
import com.twitter.heron.api.tuple.Fields;
import com.twitter.heron.api.tuple.Tuple;

import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.PackingPlan;

public class TopologyUtilityTest {
  /**
   * Create Topology proto object using HeronSubmitter API.
   *
   * @param heronConfig desired config params.
   * @param spouts spoutName -> parallelism
   * @param bolts boltName -> parallelism
   * @param connections connect default stream from value to key.
   * @return topology proto.
   */
  public static TopologyAPI.Topology createTopologyWithConnection(String topologyName,
                                                                  Config heronConfig,
                                                                  Map<String, Integer> spouts,
                                                                  Map<String, Integer> bolts,
                                                                  Map<String, String> connections) {
    TopologyBuilder builder = new TopologyBuilder();
    BaseRichSpout baseSpout = new BaseRichSpout() {
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("field1"));
      }

      public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      }

      public void nextTuple() {
      }
    };
    BaseBasicBolt basicBolt = new BaseBasicBolt() {
      public void execute(Tuple input, BasicOutputCollector collector) {
      }

      public void declareOutputFields(OutputFieldsDeclarer declarer) {
      }
    };

    for (String spout : spouts.keySet()) {
      builder.setSpout(spout, baseSpout, spouts.get(spout));
    }

    for (String bolt : bolts.keySet()) {
      BoltDeclarer boltDeclarer = builder.setBolt(bolt, basicBolt, bolts.get(bolt));
      if (connections.containsKey(bolt)) {
        boltDeclarer.shuffleGrouping(connections.get(bolt));
      }
    }

    HeronTopology heronTopology = builder.createTopology();
    try {
      HeronSubmitter.submitTopology(topologyName, heronConfig, heronTopology);
    } catch (Exception e) {
    }

    return heronTopology.
        setName(topologyName).
        setConfig(heronConfig).
        setState(TopologyAPI.TopologyState.RUNNING).
        getTopology();
  }

  public static TopologyAPI.Topology createTopology(String topologyName,
                                                    Config heronConfig,
                                                    Map<String, Integer> spouts,
                                                    Map<String, Integer> bolts) {
    return createTopologyWithConnection(
        topologyName, heronConfig, spouts, bolts, new HashMap<String, String>());
  }

  public static PackingPlan generatePacking(Map<String, List<String>> basePacking) {
    PackingPlan.Resource resource = new PackingPlan.Resource(1.0, 1 * Constants.GB, 10 * Constants.GB);

    Map<String, PackingPlan.ContainerPlan> containerPlanMap = new HashMap<>();

    for (Map.Entry<String, List<String>> entry : basePacking.entrySet()) {
      String containerId = entry.getKey();
      List<String> instanceList = entry.getValue();

      Map<String, PackingPlan.InstancePlan> instancePlanMap = new HashMap<>();

      for (String instanceId : instanceList) {
        String componentName = instanceId.split(":")[1];
        PackingPlan.InstancePlan instancePlan =
            new PackingPlan.InstancePlan(instanceId, componentName, resource);
        instancePlanMap.put(instanceId, instancePlan);
      }

      PackingPlan.ContainerPlan containerPlan =
          new PackingPlan.ContainerPlan(containerId, instancePlanMap, resource);

      containerPlanMap.put(containerId, containerPlan);
    }

    return new PackingPlan("", containerPlanMap, resource);

  }

  @Test
  public void testGetComponentParallelism() {
    int componentParallelism = 4;
    Config topologyConfig = new Config();
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", componentParallelism);
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt", componentParallelism);
    TopologyAPI.Topology topology = createTopology("testTopology", topologyConfig, spouts, bolts);
    Map<String, Integer> componentParallelismMap =
        TopologyUtility.getComponentParallelism(topology);
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
    TopologyAPI.Topology topology = createTopology("testTopology", topologyConfig, spouts, bolts);
    Assert.assertEquals((spouts.size() + bolts.size()) * componentParallelism,
        TopologyUtility.getTotalInstance(topology));
  }

  @Test
  public void testPackingToString() {
    Map<String, List<String>> packing = new HashMap<>();
    packing.put("1", Arrays.asList("1:spout:1:0", "1:bolt:3:0"));
    String packingStr = TopologyUtility.packingToString(generatePacking(packing));
    String expectedStr0 = "1:spout:1:0:bolt:3:0";
    String expectedStr1 = "1:bolt:3:0:spout:1:0";

    Assert.assertTrue(packingStr.equals(expectedStr0) || packingStr.equals(expectedStr1));

    packing.put("2", Arrays.asList("2:spout:2:1"));
    packingStr = TopologyUtility.packingToString(generatePacking(packing));

    for (String str : packingStr.split(",")) {
      if (str.startsWith("1:")) {
        // This is the packing str for container 1
        Assert.assertTrue(str.equals(expectedStr0) || str.equals(expectedStr1));
      } else if (str.startsWith("2:")) {
        // This is the packing str for container 2
        Assert.assertEquals("2:spout:2:1", str);
      } else {
        // Unexpected container string
        throw new RuntimeException("Unexpected results");
      }
    }
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
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtility.getComponentRamMap(
        createTopology("test", topologyConfig, spouts, bolts), defaultValue));  // sort it.
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
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtility.getComponentRamMap(
        createTopology("test", topologyConfig, spouts, bolts), 0));  // sort it.
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
    Map<String, Long> ramMap = new TreeMap<>(TopologyUtility.getComponentRamMap(
        createTopology("test", topologyConfig, spouts, bolts), defaultValue));  // sort it.
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
    Assert.assertFalse(TopologyUtility.verifyTopology(createTopology(
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
    Assert.assertTrue(TopologyUtility.verifyTopology(createTopologyWithConnection(
        "testTopology"  /* Bad topology name */, new Config(), spouts, bolts, connections)));
  }
}
