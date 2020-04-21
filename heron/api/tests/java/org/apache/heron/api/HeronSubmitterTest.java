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

package org.apache.heron.api;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.exception.AlreadyAliveException;
import org.apache.heron.api.exception.InvalidTopologyException;
import org.apache.heron.api.exception.TopologySubmissionException;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.common.basics.ByteAmount;

/**
 * This class covers HeronSubmitter Unit Tests for both positive and negative cases
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
public class HeronSubmitterTest {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test(expected = InvalidTopologyException.class)
  public void testInvalidTopologySubmission()
      throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = new TopologyBuilder();

    int spouts = 2;
    int bolts = 2;
    builder.setSpout("word", new TestSpout(), spouts);
    builder.setBolt("exclaim1", new TestBolt(), bolts)
        .shuffleGrouping("word");

    Config conf = new Config();
    conf.setDebug(true);

    // Put an arbitrary large number here if you don't want to slow the topology down
    conf.setMaxSpoutPending(1000 * 1000 * 1000);

    // To enable acking
    conf.setTopologyReliabilityMode(Config.TopologyReliabilityMode.ATLEAST_ONCE);
    conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-XX:+HeapDumpOnOutOfMemoryError");

    // component resource configuration
    conf.setComponentCpu("word", 0.5);
    conf.setComponentRam("word", ByteAmount.fromMegabytes(10));
    conf.setComponentDisk("word", ByteAmount.fromMegabytes(10));
    conf.setComponentCpu("exclaim1", 0.5);
    conf.setComponentRam("exclaim1", ByteAmount.fromMegabytes(10));
    conf.setComponentDisk("exclaim1", ByteAmount.fromMegabytes(10));

    // container resource configuration
    conf.setContainerDiskRequested(
        ByteAmount.fromMegabytes(10));
    conf.setContainerRamRequested(
        ByteAmount.fromMegabytes(10));
    conf.setContainerCpuRequested(1);

    // Set the number of workers or stream managers
    conf.setNumStmgrs(2);
    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }

  @Test
  @PrepareForTest(HeronSubmitter.class)
  public void testValidTopologySubmission() throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = createTopologyBuilderWithMinimumSetup();

    Config conf = new Config();

    Map<String, String> map = new HashMap();
    map.put("cmdline.topologydefn.tmpdirectory", folder.getRoot().getPath());

    PowerMockito.spy(HeronSubmitter.class);
    Mockito.when(HeronSubmitter.getHeronCmdOptions()).thenReturn(map);

    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }

  @Test(expected = TopologySubmissionException.class)
  @PrepareForTest(HeronSubmitter.class)
  public void testTopologySubmissionWhenTmpDirectoryIsEmptyPath()
      throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = createTopologyBuilderWithMinimumSetup();

    Config conf = new Config();

    Map<String, String> map = new HashMap();
    map.put("cmdline.topologydefn.tmpdirectory", "");

    PowerMockito.spy(HeronSubmitter.class);
    Mockito.when(HeronSubmitter.getHeronCmdOptions()).thenReturn(map);

    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }

  @Test(expected = TopologySubmissionException.class)
  @PrepareForTest(HeronSubmitter.class)
  public void testTopologySubmissionWhenTmpDirectoryIsSetAsInvalidPath()
      throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = createTopologyBuilderWithMinimumSetup();

    Config conf = new Config();

    Map<String, String> map = new HashMap();
    map.put("cmdline.topologydefn.tmpdirectory", "invalid_path");

    PowerMockito.spy(HeronSubmitter.class);
    Mockito.when(HeronSubmitter.getHeronCmdOptions()).thenReturn(map);

    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }

  @Test(expected = TopologySubmissionException.class)
  public void testTopologySubmissionWhenTmpDirectoryIsNotSet()
      throws AlreadyAliveException, InvalidTopologyException {
    TopologyBuilder builder = createTopologyBuilderWithMinimumSetup();
    Config conf = new Config();
    HeronSubmitter.submitTopology("test", conf, builder.createTopology());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testSubmitJar() {
    Config conf = new Config();
    HeronSubmitter.submitJar(conf, "test_jar");
  }

  private TopologyBuilder createTopologyBuilderWithMinimumSetup() {
    TopologyBuilder builder = new TopologyBuilder();

    int spouts = 2;
    int bolts = 2;
    builder.setSpout("word", new TestSpout2(), spouts);
    builder.setBolt("exclaim1", new TestBolt(), bolts).shuffleGrouping("word");
    return builder;
  }

  public static class TestSpout extends BaseRichSpout {

    private static final long serialVersionUID = -630307949908406294L;

    @SuppressWarnings("rawtypes")
    public void open(
        Map conf,
        TopologyContext context,
        SpoutOutputCollector acollector) {
    }

    public void close() {
    }

    public void nextTuple() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class TestSpout2 extends TestSpout {

    private static final long serialVersionUID = 4070649954154119533L;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class TestBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -5888421647633083078L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
  }

}
