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

import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;

import static org.mockito.Mockito.verify;

public class YarnSchedulerTest {
  @Test
  public void delegatesToDriverOnSchedule() throws Exception {
    HeronMasterDriver mockHeronDriver = Mockito.mock(HeronMasterDriver.class);
    HeronMasterDriverProvider.setInstance(mockHeronDriver);
    Mockito.doNothing().when(mockHeronDriver).launchTMaster();

    IScheduler scheduler = new YarnScheduler();
    PackingPlan mockPacking = Mockito.mock(PackingPlan.class);
    scheduler.onSchedule(mockPacking);

    InOrder invocationOrder = Mockito.inOrder(mockHeronDriver);
    invocationOrder.verify(mockHeronDriver).scheduleHeronWorkers(mockPacking);
    invocationOrder.verify(mockHeronDriver).launchTMaster();
  }

  @Test
  public void delegatesToDriverOnKill() {
    HeronMasterDriver mockHeronDriver = Mockito.mock(HeronMasterDriver.class);
    HeronMasterDriverProvider.setInstance(mockHeronDriver);

    IScheduler scheduler = new YarnScheduler();
    scheduler.onKill(null);

    verify(mockHeronDriver).killTopology();
  }
}
