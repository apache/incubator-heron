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

package org.apache.heron.scheduler.mesos.framework;

import org.junit.Assert;
import org.junit.Test;

import org.apache.mesos.Protos;


public class TaskResourcesTest {

  /**
   * Unit test for TaskResources.apply()
   */
  @Test
  public void testApplyTaskResources() throws Exception {
    final double CPU = 0.5;
    final double MEM = 1.6;
    final double DISK = 2.7;
    Protos.Offer.Builder builder =
        Protos.Offer.newBuilder()
            .setId(Protos.OfferID.newBuilder().setValue("id"))
            .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
            .setHostname("hostname")
            .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave-id"));

    Protos.Resource cpu =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.CPUS_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(CPU))
            .build();
    Protos.Resource mem =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.MEM_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(MEM))
            .build();
    Protos.Resource disk =
        Protos.Resource.newBuilder()
            .setType(Protos.Value.Type.SCALAR)
            .setName(TaskResources.DISK_RESOURCE_NAME)
            .setScalar(
                Protos.Value.Scalar.newBuilder().setValue(DISK))
            .build();
    builder.addResources(cpu).addResources(mem).addResources(disk);

    Protos.Offer offer = builder.build();

    TaskResources resources = TaskResources.apply(offer, "*");
    Assert.assertEquals(CPU, resources.getCpu(), 0.01);
    Assert.assertEquals(MEM, resources.getMem(), 0.01);
    Assert.assertEquals(DISK, resources.getDisk(), 0.01);
    Assert.assertEquals(0, resources.getPorts());
    Assert.assertEquals(0, resources.getPortsHold().size());
  }

  @Test
  public void testTaskResources() throws Exception {
    final double CPU_SMALL = 1;
    final double MEM_SMALL = 2;
    final double DISK_SMALL = 3;
    final int PORTS_SMALL = 0;

    final double CPU_BIG = 4;
    final double MEM_BIG = 5;
    final double DISK_BIG = 6;
    final int PORTS_BIG = 7;

    TaskResources small = new TaskResources(CPU_SMALL, MEM_SMALL, DISK_SMALL, PORTS_SMALL);
    TaskResources big = new TaskResources(CPU_BIG, MEM_BIG, DISK_BIG, PORTS_BIG);

    Assert.assertTrue(big.canSatisfy(small));
    Assert.assertFalse(small.canSatisfy(big));

    // Consume and calculate the remaining value
    big.consume(small);
    Assert.assertEquals(CPU_BIG - CPU_SMALL, big.getCpu(), 0.01);
    Assert.assertEquals(MEM_BIG - MEM_SMALL, big.getMem(), 0.01);
    Assert.assertEquals(DISK_BIG - DISK_SMALL, big.getDisk(), 0.01);
    Assert.assertEquals(PORTS_BIG - PORTS_SMALL, big.getPorts());
  }
}
