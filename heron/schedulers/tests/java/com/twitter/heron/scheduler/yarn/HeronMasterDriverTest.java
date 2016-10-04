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
import java.util.HashSet;
import java.util.Set;

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
import org.mockito.verification.VerificationMode;

import com.twitter.heron.packing.roundrobin.RoundRobinPacking;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.utils.PackingTestUtils;

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
    Mockito.doReturn("").when(spyDriver).getComponentRamMap();
  }

  @Test
  public void requestsEvaluatorForTMaster() throws Exception {
    AllocatedEvaluator mockEvaluator = registerMockEvaluator("testEvaluatorId", 0, 1, 1024);

    Configuration mockConfig = Mockito.mock(Configuration.class);
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig(0);

    spyDriver.scheduleTMasterContainer();
    Mockito.verify(mockEvaluator, invokedOnce()).submitContext(mockConfig);
  }

  @Test
  public void requestsEvaluatorsForWorkers() throws Exception {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();

    PackingPlan.ContainerPlan container1 = PackingTestUtils.testContainerPlan(1, 1, 2);
    containers.add(container1);
    PackingPlan.ContainerPlan container2 = PackingTestUtils.testContainerPlan(2, 1, 2, 3);
    containers.add(container2);

    AllocatedEvaluator mockEvaluator1
        = registerMockEvaluator("testEvaluatorId1", 1, getCpu(container1), getRam(container1));

    AllocatedEvaluator mockEvaluator2 =
        registerMockEvaluator("testEvaluatorId2", 2, getCpu(container2), getRam(container2));

    Configuration mockConfig = Mockito.mock(Configuration.class);
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig(1);
    Mockito.doReturn(mockConfig).when(spyDriver).createContextConfig(2);

    PackingPlan packing = new PackingPlan("packingId", containers);
    spyDriver.scheduleHeronWorkers(packing);
    Mockito.verify(mockEvaluator1, invokedOnce()).submitContext(mockConfig);
    Mockito.verify(mockEvaluator2, invokedOnce()).submitContext(mockConfig);
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
      AllocatedEvaluator mockEvaluator = registerMockEvaluator("id-" + id, id, id + 1, id + 1);
      spyDriver.launchContainerForExecutor(id, id + 1, id + 1);
      Mockito.verify(mockEvaluator, invokedOnce()).submitContext(anyConfiguration());
      mockEvaluators[id] = mockEvaluator;
    }

    spyDriver.restartTopology();

    for (int id = 0; id < numContainers; id++) {
      Mockito.verify(mockEvaluators[id]).close();
      Mockito.verify(mockEvaluators[id], invokedTwice()).submitContext(anyConfiguration());
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
      AllocatedEvaluator mockEvaluator = registerMockEvaluator("id-" + id, id, id + 1, id + 1);
      spyDriver.launchContainerForExecutor(id, id + 1, id + 1);
      Mockito.verify(mockEvaluator, invokedOnce()).submitContext(anyConfiguration());
      mockEvaluators[id] = mockEvaluator;
    }

    spyDriver.restartWorker(1);

    Mockito.verify(mockEvaluators[1]).close();
    Mockito.verify(mockEvaluators[1], invokedTwice()).submitContext(anyConfiguration());
    Mockito.verify(mockEvaluators[0], Mockito.never()).close();
    Mockito.verify(mockEvaluators[2], Mockito.never()).close();
  }

  @Test
  public void handlesFailedTMasterContainer() throws Exception {
    AllocatedEvaluator tMasterEvaluator = registerMockEvaluator("tMaster", 0, 1, 1024);

    spyDriver.scheduleTMasterContainer();
    Mockito.verify(tMasterEvaluator, invokedOnce()).submitContext(anyConfiguration());

    FailedEvaluator mockFailedContainer = Mockito.mock(FailedEvaluator.class);
    Mockito.when(mockFailedContainer.getId()).thenReturn("tMaster");
    spyDriver.new FailedContainerHandler().onNext(mockFailedContainer);

    Mockito.verify(spyDriver, invokedTwice()).allocateContainer(0, 1, 1024);
  }

  @Test
  public void handlesFailedWorkerContainer() throws Exception {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    PackingPlan.ContainerPlan container1 = PackingTestUtils.testContainerPlan(1, 1, 2);
    containers.add(container1);
    AllocatedEvaluator workerEvaluator =
        registerMockEvaluator("worker", 1, getCpu(container1), getRam(container1));

    PackingPlan packing = new PackingPlan("packingId", containers);
    spyDriver.scheduleHeronWorkers(packing);
    Mockito.verify(workerEvaluator, invokedOnce()).submitContext(anyConfiguration());

    FailedEvaluator mockFailedContainer = Mockito.mock(FailedEvaluator.class);
    Mockito.when(mockFailedContainer.getId()).thenReturn("worker");
    spyDriver.new FailedContainerHandler().onNext(mockFailedContainer);

    Mockito.verify(spyDriver, invokedTwice())
        .allocateContainer(1, getCpu(container1), getRam(container1));
  }

  @Test
  public void createsContextConfigForExecutorId() {
    Configuration config = driver.createContextConfig(4);
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

    AllocatedEvaluator evaluator = spyDriver.allocateContainer(5, 7, 234);
    Mockito.verify(mockRequestor).submit(evaluatorRequest);
    Assert.assertEquals(evaluator, mockEvaluator);
  }

  @Test(expected = HeronMasterDriver.ContainerAllocationException.class)
  public void scheduleHeronWorkersFailsOnDuplicateRequest() throws Exception {
    PackingPlan packingPlan = PackingTestUtils.testPackingPlan("test", new RoundRobinPacking());
    PackingPlan.ContainerPlan containerPlan = packingPlan.getContainers().iterator().next();
    AllocatedEvaluator container =
        registerMockEvaluator("worker", 1, getCpu(containerPlan), getRam(containerPlan));

    spyDriver.scheduleHeronWorkers(packingPlan);
    Mockito.verify(container, invokedOnce()).submitContext(anyConfiguration());

    Set<PackingPlan.ContainerPlan> toBeAddedContainerPlans = new HashSet<>();
    toBeAddedContainerPlans.add(containerPlan);
    spyDriver.scheduleHeronWorkers(toBeAddedContainerPlans);
  }

  @Test
  public void scheduleHeronWorkersAddsContainers() throws Exception {
    PackingPlan packingPlan = PackingTestUtils.testPackingPlan("test", new RoundRobinPacking());
    PackingPlan.ContainerPlan containerPlan = packingPlan.getContainers().iterator().next();
    AllocatedEvaluator container =
        registerMockEvaluator("1", 1, getCpu(containerPlan), getRam(containerPlan));
    spyDriver.scheduleHeronWorkers(packingPlan);
    Mockito.verify(container, invokedOnce()).submitContext(anyConfiguration());

    PackingPlan.ContainerPlan newContainerPlan = PackingTestUtils.testContainerPlan(2);
    int ram = getRam(newContainerPlan);
    int cpu = getCpu(newContainerPlan);
    Set<PackingPlan.ContainerPlan> toBeAddedContainerPlans = new HashSet<>();
    toBeAddedContainerPlans.add(newContainerPlan);
    toBeAddedContainerPlans.add(PackingTestUtils.testContainerPlan(3));
    AllocatedEvaluator newContainer2 = registerMockEvaluator("2", 2, cpu, ram);
    AllocatedEvaluator newContainer3 = registerMockEvaluator("3", 3, cpu, ram);

    spyDriver.scheduleHeronWorkers(toBeAddedContainerPlans);
    Mockito.verify(newContainer2, invokedOnce()).submitContext(anyConfiguration());
    Mockito.verify(newContainer3, invokedOnce()).submitContext(anyConfiguration());
  }

  @Test
  public void killWorkersTerminatesSpecificContainers() throws Exception {
    int numContainers = 5;
    AllocatedEvaluator[] mockEvaluators = new AllocatedEvaluator[numContainers];
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    for (int id = 0; id < numContainers; id++) {
      PackingPlan.ContainerPlan containerPlan = PackingTestUtils.testContainerPlan(id);
      containers.add(containerPlan);

      AllocatedEvaluator mockEvaluator =
          registerMockEvaluator("id-" + id, id, getCpu(containerPlan), getRam(containerPlan));
      mockEvaluators[id] = mockEvaluator;
    }
    PackingPlan packingPlan = new PackingPlan("packing", containers);
    spyDriver.scheduleHeronWorkers(packingPlan);

    for (int id = 0; id < numContainers; id++) {
      Mockito.verify(mockEvaluators[id], invokedOnce()).submitContext(anyConfiguration());
    }

    Set<PackingPlan.ContainerPlan> containersTobeDeleted = new HashSet<>();
    containersTobeDeleted.add(PackingTestUtils.testContainerPlan(2));
    containersTobeDeleted.add(PackingTestUtils.testContainerPlan(3));
    spyDriver.killWorkers(containersTobeDeleted);

    Mockito.verify(mockEvaluators[0], Mockito.never()).close();
    Mockito.verify(mockEvaluators[1], Mockito.never()).close();
    Mockito.verify(mockEvaluators[2]).close();
    Mockito.verify(mockEvaluators[3]).close();
    Mockito.verify(mockEvaluators[4], Mockito.never()).close();
  }

  private AllocatedEvaluator registerMockEvaluator(String name, int id, int cpu, int ram) {
    AllocatedEvaluator mockWorkerEvaluator = Mockito.mock(AllocatedEvaluator.class);
    Mockito.when(mockWorkerEvaluator.getId()).thenReturn(name);
    try {
      Mockito.doReturn(mockWorkerEvaluator).when(spyDriver).allocateContainer(id, cpu, ram);
    } catch (InterruptedException e) {
      // since this is a mock, this exception will never happen
      e.printStackTrace();
    }
    return mockWorkerEvaluator;
  }

  private VerificationMode invokedOnce() {
    return Mockito.timeout(1000).times(1);
  }

  private VerificationMode invokedTwice() {
    return Mockito.timeout(1000).times(2);
  }

  private int getRam(PackingPlan.ContainerPlan container1) {
    return (int) (container1.getRequiredResource().getRam() / Constants.MB);
  }

  private int getCpu(PackingPlan.ContainerPlan container) {
    return (int) Math.ceil(container.getRequiredResource().getCpu());
  }

  private Configuration anyConfiguration() {
    return Mockito.any(Configuration.class);
  }
}
