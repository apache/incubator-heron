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

package com.twitter.heron.scheduler.yarn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.types.NamedParameterNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

public class HeronMasterDriverTest {
  private EvaluatorRequestor mockRequestor;
  private HeronMasterDriver driver;
  private HeronMasterDriver spyDriver;

  @Before
  public void createMocks() throws IOException {
    mockRequestor = Mockito.mock(EvaluatorRequestor.class);
    driver = new HeronMasterDriver(mockRequestor,
        null,
        "yarn",
        "heron",
        "testTopology",
        "env",
        "jar",
        "package",
        "core",
        0,
        false);
    spyDriver = Mockito.spy(driver);
    Mockito.doReturn("").when(spyDriver).getPackingAsString();
    Mockito.doReturn("").when(spyDriver).getComponentRamMap();
  }

  @Test
  public void requestsEvaluatorForTMaster() throws Exception {
    AllocatedEvaluator mockEvaluator = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockEvaluator.getId()).thenReturn("testEvaluatorId");
    Mockito.doReturn(mockEvaluator).when(spyDriver).allocateContainer("0", 1, 1024);

    Configuration mockConfig = Mockito.mock(Configuration.class);
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig("0");

    spyDriver.scheduleTMasterContainer();
    Mockito.verify(mockEvaluator, Mockito.timeout(1000).times(1)).submitContext(mockConfig);
  }

  @Test
  public void requestsEvaluatorsForWorkers() throws Exception {
    AllocatedEvaluator mockEvaluator1 = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockEvaluator1.getId()).thenReturn("testEvaluatorId1");
    Mockito.doReturn(mockEvaluator1).when(spyDriver).allocateContainer("1", 2, 2048);

    AllocatedEvaluator mockEvaluator2 = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockEvaluator2.getId()).thenReturn("testEvaluatorId2");
    Mockito.doReturn(mockEvaluator2).when(spyDriver).allocateContainer("2", 4, 2050);

    Configuration mockConfig = Mockito.mock(Configuration.class);
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig("1");
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig("2");

    Map<String, PackingPlan.ContainerPlan> containers = new HashMap<>();
    addContainer("1", 2.0, 2048L, containers);
    addContainer("2", 4.0, 2050L, containers);

    PackingPlan packing = new PackingPlan("packingId", containers, null);
    spyDriver.scheduleHeronWorkers(packing);
    Mockito.verify(mockEvaluator1, Mockito.timeout(1000).times(1)).submitContext(mockConfig);
    Mockito.verify(mockEvaluator2, Mockito.timeout(1000).times(1)).submitContext(mockConfig);
  }

  private void addContainer(String id,
                            double cpu,
                            long mem,
                            Map<String, PackingPlan.ContainerPlan> containers) {
    Resource resource = new Resource(cpu, mem * 1024 * 1024, 0L);
    PackingPlan.ContainerPlan container = new PackingPlan.ContainerPlan(id, null, resource);
    containers.put(container.id, container);
  }

  @Test
  public void onKillClosesContainers() throws Exception {
    ActiveContext mockContext1 = Mockito.mock(ActiveContext.class);
    Mockito.when(mockContext1.getId()).thenReturn("0"); // TM

    ActiveContext mockContext2 = Mockito.mock(ActiveContext.class);
    Mockito.when(mockContext2.getId()).thenReturn("1"); // worker

    spyDriver.new HeronWorkerLauncher().onNext(mockContext1);
    spyDriver.new HeronWorkerLauncher().onNext(mockContext2);

    spyDriver.killTopology();

    Mockito.verify(mockContext1).close();
    Mockito.verify(mockContext2).close();
  }

  /**
   * Tests if all workers are killed and restarted
   */
  @Test
  public void onRestartClosesAndStartsContainers() throws Exception {
    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = new AllocatedEvaluator[numContainers];
    for (int id = 0; id < numContainers; id++) {
      AllocatedEvaluator mockEvaluator = Mockito.mock(AllocatedEvaluator.class);
      Mockito.when(mockEvaluator.getId()).thenReturn("container-" + id);
      Mockito.doReturn(mockEvaluator).when(spyDriver).allocateContainer("" + id, id + 1, id + 1);
      spyDriver.launchContainerForExecutor("" + id, id + 1, id + 1);
      Mockito.verify(mockEvaluator, Mockito.timeout(1000).times(1))
          .submitContext(Mockito.any(Configuration.class));
      mockEvaluators[id] = mockEvaluator;
    }

    spyDriver.restartTopology();

    for (int id = 0; id < numContainers; id++) {
      Mockito.verify(mockEvaluators[id]).close();
      Mockito.verify(mockEvaluators[id], Mockito.timeout(1000).times(2))
          .submitContext(Mockito.any(Configuration.class));
    }
  }

  /**
   * Tests if a specific worker can be killed and restarted
   */
  @Test
  public void restartsSpecificWorker() throws Exception {
    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = new AllocatedEvaluator[numContainers];
    for (int id = 0; id < numContainers; id++) {
      AllocatedEvaluator mockEvaluator = Mockito.mock(AllocatedEvaluator.class);
      Mockito.when(mockEvaluator.getId()).thenReturn("container-" + id);
      Mockito.doReturn(mockEvaluator).when(spyDriver).allocateContainer("" + id, id + 1, id + 1);
      spyDriver.launchContainerForExecutor("" + id, id + 1, id + 1);
      Mockito.verify(mockEvaluator, Mockito.timeout(1000).times(1))
          .submitContext(Mockito.any(Configuration.class));
      mockEvaluators[id] = mockEvaluator;
    }

    spyDriver.restartWorker("1");

    Mockito.verify(mockEvaluators[1]).close();
    Mockito.verify(mockEvaluators[1], Mockito.timeout(1000).times(2))
        .submitContext(Mockito.any(Configuration.class));
    Mockito.verify(mockEvaluators[0], Mockito.never()).close();
    Mockito.verify(mockEvaluators[2], Mockito.never()).close();
  }

  @Test
  public void handlesFailedTMasterContainer() throws Exception {
    AllocatedEvaluator mockTMasterEvaluator = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockTMasterEvaluator.getId()).thenReturn("tMaster");
    Mockito.doReturn(mockTMasterEvaluator).when(spyDriver).allocateContainer("0", 1, 1024);

    spyDriver.scheduleTMasterContainer();
    Mockito.verify(mockTMasterEvaluator, Mockito.timeout(1000).times(1))
        .submitContext(Mockito.any(Configuration.class));

    FailedEvaluator mockFailedContainer = Mockito.mock(FailedEvaluator.class);
    Mockito.when(mockFailedContainer.getId()).thenReturn("tMaster");
    spyDriver.new FailedContainerHandler().onNext(mockFailedContainer);

    Mockito.verify(spyDriver, Mockito.timeout(1000).times(2)).allocateContainer("0", 1, 1024);
  }

  @Test
  public void handlesFailedWorkerContainer() throws Exception {
    AllocatedEvaluator mockWorkerEvaluator = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockWorkerEvaluator.getId()).thenReturn("worker");
    Mockito.doReturn(mockWorkerEvaluator).when(spyDriver).allocateContainer("1", 1, 1024);

    Map<String, PackingPlan.ContainerPlan> containers = new HashMap<>();
    addContainer("1", 1.0, 1024L, containers);
    PackingPlan packing = new PackingPlan("packingId", containers, null);
    spyDriver.scheduleHeronWorkers(packing);
    Mockito.verify(mockWorkerEvaluator, Mockito.timeout(1000).times(1))
        .submitContext(Mockito.any(Configuration.class));

    FailedEvaluator mockFailedContainer = Mockito.mock(FailedEvaluator.class);
    Mockito.when(mockFailedContainer.getId()).thenReturn("worker");
    spyDriver.new FailedContainerHandler().onNext(mockFailedContainer);

    Mockito.verify(spyDriver, Mockito.timeout(1000).times(2)).allocateContainer("1", 1, 1024);
  }

  @Test
  public void createsContextConfigForExecutorId() {
    Configuration config = driver.createContextConfig("4");
    boolean found = false;
    for (NamedParameterNode<?> namedParameterNode : config.getNamedParameters()) {
      if (namedParameterNode.getName().equals(ContextIdentifier.class.getSimpleName())) {
        Assert.assertEquals("4", config.getNamedParameter(namedParameterNode));
        found = true;
      }
    }
    Assert.assertTrue("\"ContextIdentifier\" didn't exist.", found);
  }

  @Test
  public void requestsAndConsumesAllocatedContainer() throws Exception {
    EvaluatorRequest evaluatorRequest = driver.createEvaluatorRequest(7, 234);
    Mockito.doReturn(evaluatorRequest).when(spyDriver).createEvaluatorRequest(7, 234);

    AllocatedEvaluator mockEvaluator = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockEvaluator.getId()).thenReturn("testEvaluatorId");
    spyDriver.new ContainerAllocationHandler().onNext(mockEvaluator);

    AllocatedEvaluator evaluator = spyDriver.allocateContainer("5", 7, 234);
    Mockito.verify(mockRequestor).submit(evaluatorRequest);
    Assert.assertEquals(evaluator, mockEvaluator);
  }
}
