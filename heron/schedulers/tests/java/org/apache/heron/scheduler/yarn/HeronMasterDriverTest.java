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

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.base.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.packing.roundrobin.RoundRobinPacking;
import org.apache.heron.scheduler.SchedulerMain;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.utils.PackingTestUtils;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorDescriptor;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.types.NamedParameterNode;
import org.apache.reef.wake.time.event.StartTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
public class HeronMasterDriverTest {
  private EvaluatorRequestor mockRequestor;
  private HeronMasterDriver driver;
  private HeronMasterDriver spyDriver;

  @Before
  public void createMocks() throws IOException {
    mockRequestor = mock(EvaluatorRequestor.class);
    driver = new HeronMasterDriver(mockRequestor,
        new REEFFileNames(),
        "yarn",
        "heron",
        "testTopology",
        "env",
        "jar",
        "package",
        "core",
        0,
        false);
    spyDriver = spy(driver);
    doReturn("").when(spyDriver).getComponentRamMap();
  }

  @Test
  public void requestContainerForWorkerSubmitsValidRequest() {
    ByteAmount memory = ByteAmount.fromMegabytes(786);
    EvaluatorRequest request = spyDriver.createEvaluatorRequest(5, memory);
    doReturn(request).when(spyDriver).createEvaluatorRequest(5, memory);
    HeronMasterDriver.HeronWorker worker = new HeronMasterDriver.HeronWorker(3, 5, memory);
    spyDriver.requestContainerForWorker(3, worker);
    verify(mockRequestor, times(1)).submit(request);
  }

  @Test
  public void scheduleHeronWorkersRequestsContainersForPacking() throws Exception {
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();

    PackingPlan.ContainerPlan container1 = PackingTestUtils.testContainerPlan(1, 1, 2);
    containers.add(container1);
    PackingPlan.ContainerPlan container2 = PackingTestUtils.testContainerPlan(2, 1, 2, 3);
    containers.add(container2);

    PackingPlan packing = new PackingPlan("packingId", containers);
    spyDriver.scheduleHeronWorkers(packing);
    verify(mockRequestor, times(2)).submit(any(EvaluatorRequest.class));
    verify(spyDriver, times(1)).requestContainerForWorker(eq(1), anyHeronWorker());
    verify(spyDriver, times(1)).requestContainerForWorker(eq(2), anyHeronWorker());
    verify(spyDriver, times(1)).createEvaluatorRequest(getCpu(container1), getRam(container1));
    verify(spyDriver, times(1)).createEvaluatorRequest(getCpu(container2), getRam(container2));
  }

  @Test
  public void onKillClosesContainersKillsTMaster() throws Exception {
    HeronMasterDriver.TMaster mockTMaster = mock(HeronMasterDriver.TMaster.class);
    when(spyDriver.buildTMaster(any(ExecutorService.class))).thenReturn(mockTMaster);

    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = createApplicationWithContainers(numContainers);
    spyDriver.launchTMaster();

    spyDriver.killTopology();

    for (int id = 0; id < numContainers; id++) {
      Mockito.verify(mockEvaluators[id]).close();
      assertFalse(spyDriver.lookupByEvaluatorId("e" + id).isPresent());
    }

    verify(mockTMaster, times(1)).killTMaster();
  }

  /**
   * Tests if all workers are killed and restarted
   */
  @Test
  public void restartTopologyClosesAndStartsContainers() throws Exception {
    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = createApplicationWithContainers(numContainers);

    verify(spyDriver, never()).requestContainerForWorker(anyInt(), anyHeronWorker());
    spyDriver.restartTopology();

    for (int id = 0; id < numContainers; id++) {
      verify(spyDriver, times(1)).requestContainerForWorker(eq(id), anyHeronWorker());
      Mockito.verify(mockEvaluators[id]).close();
    }
  }

  @Test
  public void restartWorkerRestartsSpecificWorker() throws Exception {
    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = createApplicationWithContainers(numContainers);

    verify(spyDriver, never()).requestContainerForWorker(anyInt(), anyHeronWorker());
    spyDriver.restartWorker(1);

    for (int id = 0; id < numContainers; id++) {
      if (id == 1) {
        verify(spyDriver, times(1)).requestContainerForWorker(eq(id), anyHeronWorker());
        Mockito.verify(mockEvaluators[1]).close();
        assertFalse(spyDriver.lookupByEvaluatorId("e" + id).isPresent());
        continue;
      }
      verify(mockEvaluators[id], never()).close();
      assertEquals(Integer.valueOf(id), spyDriver.lookupByEvaluatorId("e" + id).get());
    }
  }

  @Test
  public void onNextFailedEvaluatorRestartsContainer() throws Exception {
    int numContainers = 3;
    AllocatedEvaluator[] mockEvaluators = createApplicationWithContainers(numContainers);

    FailedEvaluator mockFailedContainer = mock(FailedEvaluator.class);
    when(mockFailedContainer.getId()).thenReturn("e1");
    verify(spyDriver, never()).requestContainerForWorker(anyInt(), anyHeronWorker());
    spyDriver.new FailedContainerHandler().onNext(mockFailedContainer);

    for (int id = 0; id < numContainers; id++) {
      if (id == 1) {
        verify(spyDriver, times(1)).requestContainerForWorker(eq(id), anyHeronWorker());
        assertFalse(spyDriver.lookupByEvaluatorId("e" + id).isPresent());
        continue;
      }
      verify(mockEvaluators[id], never()).close();
      assertEquals(Integer.valueOf(id), spyDriver.lookupByEvaluatorId("e" + id).get());
    }
  }

  @Test
  public void createContextConfigCreatesForGivenWorkerId() {
    Configuration config = driver.createContextConfig(4);
    boolean found = false;
    for (NamedParameterNode<?> namedParameterNode : config.getNamedParameters()) {
      if (namedParameterNode.getName().equals(ContextIdentifier.class.getSimpleName())) {
        Assert.assertEquals("4", config.getNamedParameter(namedParameterNode));
        found = true;
      }
    }
    assertTrue("ContextIdentifier didn't exist.", found);
  }

  @Test(expected = HeronMasterDriver.ContainerAllocationException.class)
  public void scheduleHeronWorkersFailsOnDuplicateRequest() throws Exception {
    PackingPlan packingPlan = PackingTestUtils.testPackingPlan("test", new RoundRobinPacking());
    spyDriver.scheduleHeronWorkers(packingPlan);
    verify(spyDriver, times(1)).requestContainerForWorker(eq(1), anyHeronWorker());
    verify(mockRequestor, times(1)).submit(any(EvaluatorRequest.class));

    PackingPlan.ContainerPlan duplicatePlan = PackingTestUtils.testContainerPlan(1);
    Set<PackingPlan.ContainerPlan> toBeAddedContainerPlans = new HashSet<>();
    toBeAddedContainerPlans.add(duplicatePlan);
    spyDriver.scheduleHeronWorkers(toBeAddedContainerPlans);
  }

  @Test
  public void scheduleHeronWorkersAddsContainers() throws Exception {
    PackingPlan packingPlan = PackingTestUtils.testPackingPlan("test", new RoundRobinPacking());
    spyDriver.scheduleHeronWorkers(packingPlan);
    verify(spyDriver, times(1)).requestContainerForWorker(eq(1), anyHeronWorker());
    verify(mockRequestor, times(1)).submit(any(EvaluatorRequest.class));

    Set<PackingPlan.ContainerPlan> toBeAddedContainerPlans = new HashSet<>();
    toBeAddedContainerPlans.add(PackingTestUtils.testContainerPlan(2));
    toBeAddedContainerPlans.add(PackingTestUtils.testContainerPlan(3));

    spyDriver.scheduleHeronWorkers(toBeAddedContainerPlans);
    verify(spyDriver, times(1)).requestContainerForWorker(eq(2), anyHeronWorker());
    verify(spyDriver, times(1)).requestContainerForWorker(eq(3), anyHeronWorker());
    verify(mockRequestor, times(3)).submit(any(EvaluatorRequest.class));
  }

  @Test
  public void killWorkersTerminatesSpecificContainers() throws Exception {
    int numContainers = 5;
    Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
    for (int id = 0; id < numContainers; id++) {
      containers.add(PackingTestUtils.testContainerPlan(id));
    }
    PackingPlan packingPlan = new PackingPlan("packing", containers);

    spyDriver.scheduleHeronWorkers(packingPlan);
    for (int id = 0; id < numContainers; id++) {
      verify(spyDriver, times(1)).requestContainerForWorker(eq(id), anyHeronWorker());
      assertTrue(spyDriver.lookupByContainerPlan(id).isPresent());
    }
    verify(mockRequestor, times(numContainers)).submit(any(EvaluatorRequest.class));

    AllocatedEvaluator[] mockEvaluators = createApplicationWithContainers(numContainers);

    Set<PackingPlan.ContainerPlan> containersTobeDeleted = new HashSet<>();
    containersTobeDeleted.add(PackingTestUtils.testContainerPlan(2));
    containersTobeDeleted.add(PackingTestUtils.testContainerPlan(3));
    spyDriver.killWorkers(containersTobeDeleted);

    for (int id = 0; id < numContainers; id++) {
      if (id == 2 || id == 3) {
        verify(mockEvaluators[id], times(1)).close();
        assertFalse(spyDriver.lookupByContainerPlan(id).isPresent());
        assertFalse(spyDriver.lookupByEvaluatorId("e" + id).isPresent());
        continue;
      }
      verify(mockEvaluators[id], never()).close();
      assertTrue(spyDriver.lookupByContainerPlan(id).isPresent());
      assertTrue(spyDriver.lookupByEvaluatorId("e" + id).isPresent());
    }
  }

  @Test
  public void findLargestFittingWorkerReturnsLargestWorker() {
    Set<HeronMasterDriver.HeronWorker> workers = new HashSet<>();
    workers.add(new HeronMasterDriver.HeronWorker(1, 3, ByteAmount.fromGigabytes(3)));
    workers.add(new HeronMasterDriver.HeronWorker(2, 7, ByteAmount.fromGigabytes(7)));
    workers.add(new HeronMasterDriver.HeronWorker(3, 5, ByteAmount.fromGigabytes(5)));
    workers.add(new HeronMasterDriver.HeronWorker(4, 1, ByteAmount.fromGigabytes(1)));

    // enough memory and cores to fit largest container, 2
    verifyFittingContainer(workers, 7 * 1024 + 100, 7, 2);

    // enough to fit 3 but not container 2
    verifyFittingContainer(workers, 5 * 1024 + 100, 6, 3);

    // enough memory but not enough cores for container 2
    verifyFittingContainer(workers, 7 * 1024 + 100, 5, 3);

    // enough cores but not enough memory for container 2
    verifyFittingContainer(workers, 5 * 1024 + 100, 7, 3);
  }

  private void verifyFittingContainer(Set<HeronMasterDriver.HeronWorker> containers,
                                      int ram,
                                      int cores,
                                      int expectedContainer) {
    EvaluatorDescriptor evaluatorDescriptor = mock(EvaluatorDescriptor.class);
    AllocatedEvaluator mockEvaluator = mock(AllocatedEvaluator.class);
    when(mockEvaluator.getEvaluatorDescriptor()).thenReturn(evaluatorDescriptor);

    when(evaluatorDescriptor.getMemory()).thenReturn(ram);
    when(evaluatorDescriptor.getNumberOfCores()).thenReturn(cores);
    Optional<HeronMasterDriver.HeronWorker> worker =
        spyDriver.findLargestFittingWorker(mockEvaluator, containers, false);
    assertTrue(worker.isPresent());
    assertEquals(expectedContainer, worker.get().getWorkerId());
  }

  @Test
  public void fitBiggestContainerIgnoresCoresIfMissing() {
    Set<HeronMasterDriver.HeronWorker> workers = new HashSet<>();
    workers.add(new HeronMasterDriver.HeronWorker(1, 3, ByteAmount.fromGigabytes(3)));

    AllocatedEvaluator mockEvaluator = createMockEvaluator("test", 1, ByteAmount.fromGigabytes(3));
    Optional<HeronMasterDriver.HeronWorker> result =
        spyDriver.findLargestFittingWorker(mockEvaluator, workers, false);
    Assert.assertFalse(result.isPresent());

    result = spyDriver.findLargestFittingWorker(mockEvaluator, workers, true);
    assertTrue(result.isPresent());
    assertEquals(1, result.get().getWorkerId());
  }

  @Test
  public void onNextAllocatedEvaluatorStartsWorker() throws Exception {
    PackingPlan packingPlan = PackingTestUtils.testPackingPlan("test", new RoundRobinPacking());
    spyDriver.scheduleHeronWorkers(packingPlan);

    assertTrue(spyDriver.lookupByContainerPlan(1).isPresent());
    PackingPlan.ContainerPlan containerPlan = spyDriver.lookupByContainerPlan(1).get();

    AllocatedEvaluator mockEvaluator =
        createMockEvaluator("test", getCpu(containerPlan), getRam(containerPlan));

    assertFalse(spyDriver.lookupByEvaluatorId("test").isPresent());
    spyDriver.new ContainerAllocationHandler().onNext(mockEvaluator);
    assertTrue(spyDriver.lookupByEvaluatorId("test").isPresent());
    assertEquals(Integer.valueOf(1), spyDriver.lookupByEvaluatorId("test").get());
    verify(mockEvaluator, times(1)).submitContext(any(Configuration.class));
  }

  @Test
  public void onNextAllocatedEvaluatorDiscardsExtraWorker() throws Exception {
    AllocatedEvaluator mockEvaluator
        = createMockEvaluator("test", 1, ByteAmount.fromMegabytes(123));
    assertFalse(spyDriver.lookupByEvaluatorId("test").isPresent());
    spyDriver.new ContainerAllocationHandler().onNext(mockEvaluator);
    assertFalse(spyDriver.lookupByEvaluatorId("test").isPresent());
    verify(mockEvaluator, never()).submitContext(any(Configuration.class));
  }

  @Test
  public void tMasterLaunchLaunchesExecutorForTMaster() throws Exception {
    ExecutorService executorService = mock(ExecutorService.class);
    HeronMasterDriver.TMaster tMaster = spyDriver.buildTMaster(executorService);
    doReturn(mock(Future.class)).when(executorService).submit(tMaster);
    tMaster.launch();
    verify(executorService, times(1)).submit(tMaster);
  }

  @Test
  public void tMasterKillTerminatesTMaster() throws Exception {
    ExecutorService mockExecutorService = mock(ExecutorService.class);
    HeronMasterDriver.TMaster tMaster = spyDriver.buildTMaster(mockExecutorService);

    Future<?> mockFuture = mock(Future.class);
    doReturn(mockFuture).when(mockExecutorService).submit(tMaster);

    tMaster.launch();
    tMaster.killTMaster();

    verify(mockFuture, times(1)).cancel(true);
    verify(mockExecutorService, times(1)).shutdownNow();
  }

  @Test
  public void tMasterLaunchRestartsTMasterOnFailure() throws Exception {
    HeronMasterDriver.TMaster tMaster =
        spy(spyDriver.buildTMaster(Executors.newSingleThreadExecutor()));

    HeronExecutorTask mockTask = mock(HeronExecutorTask.class);
    final CountDownLatch testLatch = new CountDownLatch(1);
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        testLatch.await();
        return null;
      }
    }).when(mockTask).startExecutor();
    doReturn(mockTask).when(tMaster).getTMasterExecutorTask();

    tMaster.launch();

    verify(mockTask, timeout(1000).times(1)).startExecutor();
    testLatch.countDown();
    //retries if tmaster ends for some reason
    verify(mockTask, timeout(1000).times(3)).startExecutor();
  }

  @Test
  @PrepareForTest({HeronReefUtils.class, SchedulerMain.class})
  public void onNextStartTimeStartsSchedulerTMaster() throws Exception {
    PowerMockito.spy(HeronReefUtils.class);
    PowerMockito.doNothing().when(HeronReefUtils.class,
        "extractPackageInSandbox",
        anyString(),
        anyString(),
        anyString());

    SchedulerMain mockScheduler = mock(SchedulerMain.class);
    PowerMockito.spy(SchedulerMain.class);
    PowerMockito.doReturn(mockScheduler).when(SchedulerMain.class,
        "createInstance",
        anyString(),
        anyString(),
        anyString(),
        anyString(),
        anyString(),
        eq(0),
        eq(false));

    spyDriver.new HeronSchedulerLauncher().onNext(new StartTime(System.currentTimeMillis()));

    verify(mockScheduler, times(1)).runScheduler();
  }

  private AllocatedEvaluator[] createApplicationWithContainers(int numContainers) {
    AllocatedEvaluator[] mockEvaluators = new AllocatedEvaluator[numContainers];
    for (int id = 0; id < numContainers; id++) {
      mockEvaluators[id]
          = simulateContainerAllocation("e" + id, 1, ByteAmount.fromMegabytes(123), id);
    }
    for (int id = 0; id < numContainers; id++) {
      assertEquals(Integer.valueOf(id), spyDriver.lookupByEvaluatorId("e" + id).get());
      verify(mockEvaluators[id], times(1)).submitContext(anyConfiguration());
      verify(mockEvaluators[id], never()).close();
    }
    return mockEvaluators;
  }

  private AllocatedEvaluator simulateContainerAllocation(String evaluatorId,
                                                         int cores,
                                                         ByteAmount ram,
                                                         int workerId) {
    AllocatedEvaluator evaluator = createMockEvaluator(evaluatorId, cores, ram);
    HeronMasterDriver.HeronWorker worker = new HeronMasterDriver.HeronWorker(workerId, cores, ram);

    Set<HeronMasterDriver.HeronWorker> workers = new HashSet<>();
    workers.add(worker);
    doReturn(workers).when(spyDriver).getWorkersAwaitingAllocation();

    doReturn(Optional.of(worker)).when(spyDriver)
        .findLargestFittingWorker(eq(evaluator), eq(workers), eq(false));

    spyDriver.new ContainerAllocationHandler().onNext(evaluator);
    return evaluator;
  }

  private AllocatedEvaluator createMockEvaluator(String evaluatorId, int cores, ByteAmount mem) {
    EvaluatorDescriptor descriptor = mock(EvaluatorDescriptor.class);
    when(descriptor.getMemory()).thenReturn(((Long) mem.asMegabytes()).intValue());
    when(descriptor.getNumberOfCores()).thenReturn(cores);
    AllocatedEvaluator mockEvaluator = mock(AllocatedEvaluator.class);
    when(mockEvaluator.getEvaluatorDescriptor()).thenReturn(descriptor);
    when(mockEvaluator.getId()).thenReturn(evaluatorId);
    return mockEvaluator;
  }

  private ByteAmount getRam(PackingPlan.ContainerPlan container) {
    return container.getRequiredResource().getRam();
  }

  private int getCpu(PackingPlan.ContainerPlan container) {
    return (int) Math.ceil(container.getRequiredResource().getCpu());
  }

  private Configuration anyConfiguration() {
    return Mockito.any(Configuration.class);
  }

  private HeronMasterDriver.HeronWorker anyHeronWorker() {
    return any(HeronMasterDriver.HeronWorker.class);
  }
}
