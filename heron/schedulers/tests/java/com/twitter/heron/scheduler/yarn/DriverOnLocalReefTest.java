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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.reef.client.ClientConfiguration;
import org.apache.reef.client.DriverConfiguration;
import org.apache.reef.client.REEF;
import org.apache.reef.client.RunningJob;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.runtime.local.client.LocalRuntimeConfiguration;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.junit.Test;

import com.twitter.heron.scheduler.yarn.DriverOnLocalReefTest.TestDriver.Activated;
import com.twitter.heron.scheduler.yarn.DriverOnLocalReefTest.TestDriver.Allocated;
import com.twitter.heron.scheduler.yarn.DriverOnLocalReefTest.TestDriver.DriverStarter;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.Resource;

public class DriverOnLocalReefTest {
  @Test
  public void requestContainersForTMasterAndWorkers() throws Exception {
    Configuration runtimeConf = LocalRuntimeConfiguration.CONF.build();

    Configuration reefClientConf = ClientConfiguration.CONF
        .set(ClientConfiguration.ON_JOB_RUNNING, ClientListener.JobRunning.class)
        .build();

    Configuration driverConf = DriverConfiguration.CONF
        .set(DriverConfiguration.ON_DRIVER_STARTED, DriverStarter.class)
        .set(DriverConfiguration.ON_EVALUATOR_ALLOCATED, Allocated.class)
        .set(DriverConfiguration.ON_CONTEXT_ACTIVE, Activated.class)
        .build();

    final Injector injector = Tang.Factory.getTang().newInjector(runtimeConf, reefClientConf);
    final REEF reef = injector.getInstance(REEF.class);

    final ClientListener clientListener = injector.getInstance(ClientListener.class);
    clientListener.jobWatcher = new CountDownLatch(1);
    reef.submit(driverConf);
    clientListener.jobWatcher.await(1, TimeUnit.SECONDS);
    // The local implementation can fail to find JAVA_HOME. It can cause this test to fail.
    // Uncomment the verification steps below to test it manually
//    Assert.assertEquals(0, clientListener.jobWatcher.getCount());
  }

  @Unit
  static class ClientListener {
    private CountDownLatch jobWatcher = new CountDownLatch(1);

    @Inject
    ClientListener() {
    }

    class JobRunning implements EventHandler<RunningJob> {
      @Override
      public void onNext(RunningJob runningJob) {
        System.out.println("job is running");
        jobWatcher.countDown();
      }
    }
  }

  @Unit
  static class TestDriver {
    private HeronMasterDriver driver;
    private CountDownLatch counter;

    @Inject
    TestDriver(EvaluatorRequestor requestor) throws IOException {
      driver = new HeronMasterDriver(requestor, null, "", "", "", "", null, null, null, 0, false);
    }

    private void addContainer(String id,
                              double cpu,
                              long mem,
                              Set<PackingPlan.ContainerPlan> containers) {
      Resource resource = new Resource(cpu, mem * 1024 * 1024, 0L);
      PackingPlan.ContainerPlan container = new PackingPlan.ContainerPlan(id, null, resource);
      containers.add(container);
    }

    class DriverStarter implements EventHandler<StartTime> {
      @Override
      public void onNext(StartTime startTime) {
        try {
          counter = new CountDownLatch(2);
          driver.scheduleTMasterContainer();
          Set<PackingPlan.ContainerPlan> containers = new HashSet<>();
          addContainer("1", 1.0, 512L, containers);
          PackingPlan packing = new PackingPlan("packingId", containers, null);
          driver.scheduleHeronWorkers(packing);
          counter.await(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } catch (HeronMasterDriver.ContainerAllocationException e) {
          throw new RuntimeException(e);
        }
      }
    }

    class Allocated implements EventHandler<AllocatedEvaluator> {
      @Override
      public void onNext(AllocatedEvaluator evaluator) {
        driver.new ContainerAllocationHandler().onNext(evaluator);
      }
    }

    class Activated implements EventHandler<ActiveContext> {
      @Override
      public void onNext(ActiveContext activeContext) {
        counter.countDown();
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        activeContext.close();
      }
    }
  }
}
