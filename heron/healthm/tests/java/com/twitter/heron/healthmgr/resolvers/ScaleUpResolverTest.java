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

package com.twitter.heron.healthmgr.resolvers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.detector.Symptom;
import com.microsoft.dhalion.diagnoser.Diagnosis;
import com.microsoft.dhalion.metrics.ComponentMetrics;
import com.microsoft.dhalion.metrics.InstanceMetrics;
import com.microsoft.dhalion.resolver.Action;

import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.healthmgr.common.HealthMgrConstants;
import com.twitter.heron.healthmgr.common.PackingPlanProvider;
import com.twitter.heron.healthmgr.common.TopologyProvider;
import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.proto.scheduler.Scheduler.UpdateTopologyRequest;
import com.twitter.heron.scheduler.client.ISchedulerClient;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;
import com.twitter.heron.spi.packing.IRepacking;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.TopologyTests;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class ScaleUpResolverTest {
  static final String BACK_PRESSURE = HealthMgrConstants.METRIC_INSTANCE_BACK_PRESSURE;

  @Test
  public void testResolve() {
    TopologyAPI.Topology topology = createTestTopology();
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.get()).thenReturn(currentPlan);

    ISchedulerClient scheduler = mock(ISchedulerClient.class);
    when(scheduler.updateTopology(any(UpdateTopologyRequest.class))).thenReturn(true);

    ComponentMetrics metrics = new ComponentMetrics("bolt", "i1", BACK_PRESSURE, 123);
    Symptom symptom = new Symptom(BACK_PRESSURE, metrics);
    List<Diagnosis> diagnosis = new ArrayList<>();
    diagnosis.add(new Diagnosis("test", symptom));

    ScaleUpResolver resolver
        = new ScaleUpResolver(null, packingPlanProvider, scheduler, null);
    ScaleUpResolver spyResolver = spy(resolver);

    doReturn(2).when(spyResolver).computeScaleUpFactor(metrics);
    doReturn(currentPlan).when(spyResolver).buildNewPackingPlan(any(HashMap.class), eq(currentPlan));

    List<Action> result = spyResolver.resolve(diagnosis);
    verify(scheduler, times(1)).updateTopology(any(UpdateTopologyRequest.class));
    assertEquals(1, result.size());
  }

  @Test
  public void testBuildPackingPlan() {
    TopologyAPI.Topology topology = createTestTopology();
    TopologyProvider topologyProvider = createTopologyProvider(topology);
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    Map<String, Integer> changeRequest = new HashMap<>();
    changeRequest.put("bolt-2", 4);

    Map<String, Integer> deltaChange = new HashMap<>();
    deltaChange.put("bolt-2", 3);

    IRepacking repacking = mock(IRepacking.class);
    when(repacking.repack(currentPlan, deltaChange)).thenReturn(currentPlan);

    ScaleUpResolver resolver = new ScaleUpResolver(topologyProvider, null, null, config);
    ScaleUpResolver spyResolver = spy(resolver);
    doReturn(repacking).when(spyResolver).getRepackingClass("Repacking");

    PackingPlan newPlan = spyResolver.buildNewPackingPlan(changeRequest, currentPlan);
    assertEquals(currentPlan, newPlan);
  }

  private PackingPlan createPacking(TopologyAPI.Topology topology, Config config) {
    RoundRobinPacking packing = new RoundRobinPacking();
    packing.initialize(config, topology);
    return packing.pack();
  }

  private Config createConfig(TopologyAPI.Topology topology) {
    return Config.newBuilder(true)
        .put(Key.TOPOLOGY_ID, topology.getId())
        .put(Key.TOPOLOGY_NAME, topology.getName())
        .put(Key.REPACKING_CLASS, "Repacking")
        .build();
  }

  private TopologyProvider createTopologyProvider(TopologyAPI.Topology topology) {
    TopologyProvider topologyProvider = mock(TopologyProvider.class);
    when(topologyProvider.get()).thenReturn(topology);
    return topologyProvider;
  }

  private TopologyAPI.Topology createTestTopology() {
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt-1", 1);
    bolts.put("bolt-2", 1);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", 1);
    return TopologyTests.createTopology("T", new com.twitter.heron.api.Config(), spouts, bolts);
  }

  @Test
  public void testScaleUpFactorComputation() {
    ScaleUpResolver resolver = new ScaleUpResolver(null, null, null, null);

    ComponentMetrics metrics = new ComponentMetrics("bolt");
    metrics.addInstanceMetric(new InstanceMetrics("i1", BACK_PRESSURE, 500));
    metrics.addInstanceMetric(new InstanceMetrics("i2", BACK_PRESSURE, 0));

    int result = resolver.computeScaleUpFactor(metrics);
    assertEquals(4, result);

    metrics = new ComponentMetrics("bolt");
    metrics.addInstanceMetric(new InstanceMetrics("i1", BACK_PRESSURE, 750));
    metrics.addInstanceMetric(new InstanceMetrics("i2", BACK_PRESSURE, 0));

    result = resolver.computeScaleUpFactor(metrics);
    assertEquals(8, result);

    metrics = new ComponentMetrics("bolt");
    metrics.addInstanceMetric(new InstanceMetrics("i1", BACK_PRESSURE, 400));
    metrics.addInstanceMetric(new InstanceMetrics("i2", BACK_PRESSURE, 100));
    metrics.addInstanceMetric(new InstanceMetrics("i3", BACK_PRESSURE, 0));

    result = resolver.computeScaleUpFactor(metrics);
    assertEquals(6, result);
  }
}
