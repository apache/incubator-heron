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
package org.apache.heron.resource;

import java.util.ArrayList;
import java.util.List;

import org.junit.Ignore;

import org.apache.heron.api.Config;
import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.proto.system.PhysicalPlans;

@Ignore
public final class MockPhysicalPlansBuilder {

  private PhysicalPlans.PhysicalPlan.Builder pPlan;
  private TopologyBuilder topologyBuilder;
  private List<PhysicalPlans.Instance.Builder> instanceBuilders;

  private Config conf;
  private TopologyAPI.TopologyState initialTopologyState;

  private MockPhysicalPlansBuilder() {
    pPlan = PhysicalPlans.PhysicalPlan.newBuilder();
    topologyBuilder = new TopologyBuilder();
    instanceBuilders = new ArrayList<>();

    conf = null;
    initialTopologyState = null;
  }

  public static MockPhysicalPlansBuilder newBuilder() {
    return new MockPhysicalPlansBuilder();
  }

  public MockPhysicalPlansBuilder withTopologyConfig(
      Config.TopologyReliabilityMode reliabilityMode,
      int messageTimeout
  ) {
    conf = new Config();
    conf.setTeamEmail("streaming-compute@twitter.com");
    conf.setTeamName("stream-computing");
    conf.setTopologyProjectName("heron-integration-test");
    conf.setNumStmgrs(1);
    conf.setMaxSpoutPending(100);
    conf.setTopologyReliabilityMode(reliabilityMode);
    if (messageTimeout != -1) {
      conf.setMessageTimeoutSecs(messageTimeout);
      conf.put("topology.enable.message.timeouts", "true");
    }

    return this;
  }

  public MockPhysicalPlansBuilder withTopologyState(TopologyAPI.TopologyState topologyState) {
    initialTopologyState = topologyState;
    return this;
  }

  public MockPhysicalPlansBuilder withSpoutInstance(
      String componentName,
      int taskId,
      String instanceId,
      IRichSpout spout
  ) {
    PhysicalPlans.InstanceInfo.Builder spoutInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    spoutInstanceInfo.setComponentName(componentName);
    spoutInstanceInfo.setTaskId(taskId);
    spoutInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder spoutInstance = PhysicalPlans.Instance.newBuilder();
    spoutInstance.setInstanceId(instanceId);
    spoutInstance.setStmgrId("stream-manager-id");
    spoutInstance.setInfo(spoutInstanceInfo);

    topologyBuilder.setSpout(componentName, spout, 1);
    instanceBuilders.add(spoutInstance);

    return this;
  }

  public MockPhysicalPlansBuilder withBoltInstance(
      String componentName,
      int taskId,
      String instanceId,
      String upStreamComponentId,
      IRichBolt bolt
  ) {
    PhysicalPlans.InstanceInfo.Builder boltInstanceInfo = PhysicalPlans.InstanceInfo.newBuilder();
    boltInstanceInfo.setComponentName(componentName);
    boltInstanceInfo.setTaskId(taskId);
    boltInstanceInfo.setComponentIndex(0);

    PhysicalPlans.Instance.Builder boltInstance = PhysicalPlans.Instance.newBuilder();
    boltInstance.setInstanceId(instanceId);
    boltInstance.setStmgrId("stream-manager-id");
    boltInstance.setInfo(boltInstanceInfo);

    topologyBuilder.setBolt(componentName, bolt, 1)
        .shuffleGrouping(upStreamComponentId);
    instanceBuilders.add(boltInstance);

    return this;
  }

  public PhysicalPlans.PhysicalPlan build() {
    addStmgrs();
    pPlan.setTopology(buildTopology());

    for (PhysicalPlans.Instance.Builder b : instanceBuilders) {
      pPlan.addInstances(b);
    }

    return pPlan.build();
  }

  private TopologyAPI.Topology buildTopology() {
    return topologyBuilder
        .createTopology()
        .setName("topology-name")
        .setConfig(conf)
        .setState(initialTopologyState)
        .getTopology();
  }

  private void addStmgrs() {
    PhysicalPlans.StMgr.Builder stmgr = PhysicalPlans.StMgr.newBuilder();
    stmgr.setId("stream-manager-id");
    stmgr.setHostName("127.0.0.1");
    stmgr.setDataPort(8888);
    stmgr.setLocalEndpoint("endpoint");
    pPlan.addStmgrs(stmgr);
  }

}
