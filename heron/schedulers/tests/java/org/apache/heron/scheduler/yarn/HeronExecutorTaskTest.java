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

package org.apache.heron.scheduler.yarn;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.bolt.BaseBasicBolt;
import org.apache.heron.api.bolt.BasicOutputCollector;
import org.apache.heron.api.generated.TopologyAPI.Topology;
import org.apache.heron.api.generated.TopologyAPI.TopologyState;
import org.apache.heron.api.spout.BaseRichSpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.utils.ShellUtils;
import org.apache.reef.runtime.common.files.REEFFileNames;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.swing.*", "jdk.internal.reflect.*"})
public class HeronExecutorTaskTest {
  @Test
  public void providesConfigsNeededForExecutorCmd() throws Exception {
    Topology testTopology = createTestTopology("testTopology");

    HeronExecutorTask spyTask = getSpyOnHeronExecutorTask(null);

    Mockito.doReturn("file").when(spyTask).getTopologyDefnFile();
    Mockito.doReturn(testTopology).when(spyTask).getTopology("file");
    String[] command = spyTask.getExecutorCommand();

    // only two configs; state manager root and url should be null.
    int nullCounter = 2;
    for (String subCommand : command) {
      String[] flagArg = SchedulerUtils.splitCommandArg(subCommand);
      if (flagArg.length > 1 && flagArg[1].equals("null")) {
        nullCounter--;
      }
    }
    Assert.assertEquals(0, nullCounter);
  }

  /**
   * Tests launcher execution by yarn task
   */
  @Test
  @PrepareForTest({ShellUtils.class, HeronReefUtils.class, REEFFileNames.class})
  public void setsEnvironmentForExecutor() throws Exception {
    PowerMockito.spy(HeronReefUtils.class);
    PowerMockito.doNothing().when(HeronReefUtils.class,
        "extractPackageInSandbox",
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyString());

    REEFFileNames mockFiles = PowerMockito.mock(REEFFileNames.class);
    File global = new File(".");
    PowerMockito.when(mockFiles.getGlobalFolder()).thenReturn(global);
    HeronExecutorTask spyTask = getSpyOnHeronExecutorTask(mockFiles);
    String[] testCmd = {"cmd"};
    Mockito.doReturn(testCmd).when(spyTask).getExecutorCommand();

    HashMap<String, String> env = spyTask.getEnvironment("testCWD");
    Assert.assertEquals(1, env.size());
    String pexRoot = env.get("PEX_ROOT");
    Assert.assertNotNull(pexRoot);
    Assert.assertEquals("testCWD", pexRoot);

    Mockito.when(spyTask.getEnvironment(Mockito.anyString())).thenReturn(env);
    Process mockProcess = Mockito.mock(Process.class);
    Mockito.doReturn(0).when(mockProcess).waitFor();

    PowerMockito.spy(ShellUtils.class);
    PowerMockito.doReturn(mockProcess).when(
        ShellUtils.class,
        "runASyncProcess",
        Mockito.eq(testCmd),
        Mockito.any(File.class),
        Mockito.eq(env),
        Mockito.any(String.class),
        Mockito.any(Boolean.class));
    spyTask.call(null);
    Mockito.verify(mockProcess).waitFor();
  }

  private HeronExecutorTask getSpyOnHeronExecutorTask(REEFFileNames mockFiles) {
    HeronExecutorTask task = new HeronExecutorTask(mockFiles,
        5,
        "cluster",
        "role",
        "testTopology",
        "env",
        "package",
        "core",
        "topology.jar",
        "componentRamMap",
        false);
    return Mockito.spy(task);
  }

  Topology createTestTopology(String name) {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("spout-1", new TestSpout(), 2);
    builder.setBolt("bolt-1", new TestBolt(), 1).shuffleGrouping("spout-1");
    HeronTopology topology = builder.createTopology();
    org.apache.heron.api.Config config = new org.apache.heron.api.Config();
    return topology.setName(name).setConfig(config).setState(TopologyState.RUNNING).getTopology();
  }

  public static class TestBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }

  public static class TestSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;

    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
    }

    @Override
    public void nextTuple() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
  }
}
