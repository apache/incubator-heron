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

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;

public class YarnSchedulerTest {
  @Test
  @PrepareForTest(HeronMasterDriverProvider.class)
  public void delegatesToDriverOnSchedule() {
    HeronMasterDriver mockHeronDriver = Mockito.mock(HeronMasterDriver.class);
    HeronMasterDriverProvider.setInstance(mockHeronDriver);

    IScheduler scheduler = new YarnScheduler();
    PackingPlan mockPacking = Mockito.mock(PackingPlan.class);
    scheduler.onSchedule(mockPacking);

    InOrder invocationOrder = Mockito.inOrder(mockHeronDriver);
    invocationOrder.verify(mockHeronDriver).scheduleTMasterContainer();
    invocationOrder.verify(mockHeronDriver).scheduleHeronWorkers(mockPacking);
  }

  @Test
  @PrepareForTest(HeronMasterDriverProvider.class)
  public void delegatesToDriverOnKill() {
    HeronMasterDriver mockHeronDriver = Mockito.mock(HeronMasterDriver.class);
    HeronMasterDriverProvider.setInstance(mockHeronDriver);

    IScheduler scheduler = new YarnScheduler();
    scheduler.onKill(null);

    Mockito.verify(mockHeronDriver).killTopology();
  }
}
