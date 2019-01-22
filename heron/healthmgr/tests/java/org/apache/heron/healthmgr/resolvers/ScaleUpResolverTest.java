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

package org.apache.heron.healthmgr.resolvers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.dhalion.core.Action;
import com.microsoft.dhalion.core.Diagnosis;
import com.microsoft.dhalion.core.Measurement;
import com.microsoft.dhalion.core.MeasurementsTable;
import com.microsoft.dhalion.events.EventManager;
import com.microsoft.dhalion.policy.PoliciesExecutor.ExecutionContext;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.utils.topology.TopologyTests;
import org.apache.heron.healthmgr.common.PackingPlanProvider;
import org.apache.heron.healthmgr.common.PhysicalPlanProvider;
import org.apache.heron.packing.roundrobin.RoundRobinPacking;
import org.apache.heron.proto.scheduler.Scheduler.UpdateTopologyRequest;
import org.apache.heron.proto.system.PhysicalPlans.PhysicalPlan;
import org.apache.heron.scheduler.client.ISchedulerClient;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.IRepacking;
import org.apache.heron.spi.packing.PackingPlan;

import static org.apache.heron.healthmgr.diagnosers.BaseDiagnoser.DiagnosisType.DIAGNOSIS_UNDER_PROVISIONING;
import static org.apache.heron.healthmgr.sensors.BaseSensor.MetricName.METRIC_BACK_PRESSURE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScaleUpResolverTest {
  private EventManager eventManager = new EventManager();

  @Test
  public void testResolve() {
    TopologyAPI.Topology topology = createTestTopology();
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    PackingPlanProvider packingPlanProvider = mock(PackingPlanProvider.class);
    when(packingPlanProvider.get()).thenReturn(currentPlan);

    ISchedulerClient scheduler = mock(ISchedulerClient.class);
    when(scheduler.updateTopology(any(UpdateTopologyRequest.class))).thenReturn(true);

    Instant now = Instant.now();
    Collections.singletonList(new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 123));

    List<String> assignments = Collections.singletonList("bolt");
    Diagnosis diagnoses =
        new Diagnosis(DIAGNOSIS_UNDER_PROVISIONING.text(), now, assignments, null);
    List<Diagnosis> diagnosis = Collections.singletonList(diagnoses);

    ExecutionContext context = mock(ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);

    ScaleUpResolver resolver
        = new ScaleUpResolver(null, packingPlanProvider, scheduler, eventManager, null);
    resolver.initialize(context);
    ScaleUpResolver spyResolver = spy(resolver);

    doReturn(2).when(spyResolver).computeScaleUpFactor("bolt");
    doReturn(currentPlan).when(spyResolver)
        .buildNewPackingPlan(any(HashMap.class), eq(currentPlan));

    Collection<Action> result = spyResolver.resolve(diagnosis);
    verify(scheduler, times(1)).updateTopology(any(UpdateTopologyRequest.class));
    assertEquals(1, result.size());
  }

  @Test
  public void testBuildPackingPlan() {
    TopologyAPI.Topology topology = createTestTopology();
    PhysicalPlanProvider topologyProvider = createPhysicalPlanProvider(topology);
    Config config = createConfig(topology);
    PackingPlan currentPlan = createPacking(topology, config);

    Map<String, Integer> changeRequest = new HashMap<>();
    changeRequest.put("bolt-2", 4);

    Map<String, Integer> deltaChange = new HashMap<>();
    deltaChange.put("bolt-2", 3);

    IRepacking repacking = mock(IRepacking.class);
    when(repacking.repack(currentPlan, deltaChange)).thenReturn(currentPlan);

    ScaleUpResolver resolver =
        new ScaleUpResolver(topologyProvider, null, null, eventManager, config);
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

  private PhysicalPlanProvider createPhysicalPlanProvider(TopologyAPI.Topology topology) {
    PhysicalPlan pp = PhysicalPlan.newBuilder().setTopology(topology).build();

    PhysicalPlanProvider physicalPlanProvider = mock(PhysicalPlanProvider.class);
    when(physicalPlanProvider.get()).thenReturn(pp);
    return physicalPlanProvider;
  }

  private TopologyAPI.Topology createTestTopology() {
    Map<String, Integer> bolts = new HashMap<>();
    bolts.put("bolt-1", 1);
    bolts.put("bolt-2", 1);
    Map<String, Integer> spouts = new HashMap<>();
    spouts.put("spout", 1);
    return TopologyTests.createTopology("T", new org.apache.heron.api.Config(), spouts, bolts);
  }

  @Test
  public void testScaleUpFactorComputation() {
    Instant now = Instant.now();
    Collection<Measurement> result = new ArrayList<>();

    ExecutionContext context = Mockito.mock(ExecutionContext.class);
    when(context.checkpoint()).thenReturn(now);
    when(context.previousCheckpoint()).thenReturn(now);

    ScaleUpResolver resolver = new ScaleUpResolver(null, null, null, eventManager, null);
    resolver.initialize(context);

    result.add(new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 500));
    result.add(new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), now, 0));
    when(context.measurements()).thenReturn(MeasurementsTable.of(result));
    int factor = resolver.computeScaleUpFactor("bolt");
    assertEquals(4, factor);

    result.clear();
    result.add(new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 750));
    result.add(new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), now, 0));
    when(context.measurements()).thenReturn(MeasurementsTable.of(result));
    factor = resolver.computeScaleUpFactor("bolt");
    assertEquals(8, factor);

    result.clear();
    result.add(new Measurement("bolt", "i1", METRIC_BACK_PRESSURE.text(), now, 400));
    result.add(new Measurement("bolt", "i2", METRIC_BACK_PRESSURE.text(), now, 100));
    result.add(new Measurement("bolt", "i3", METRIC_BACK_PRESSURE.text(), now, 0));
    when(context.measurements()).thenReturn(MeasurementsTable.of(result));
    factor = resolver.computeScaleUpFactor("bolt");
    assertEquals(6, factor);
  }
}
