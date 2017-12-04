//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.scheduler.nomad;

import java.io.IOException;

//import com.hashicorp.nomad.javasdk.NomadApiClient;
//import com.hashicorp.nomad.javasdk.NomadApiConfiguration;
//import com.hashicorp.nomad.javasdk.NomadException;

import org.junit.Test;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Key;

public class NomadSchedulerTest {

  @Test
  public void testScheduler() throws InterruptedException, IOException {

//    NomadScheduler nomadScheduler = new NomadScheduler();
//
//    Config config = Config.newBuilder()
//        .build();
//
//    Config runtime = Config.newBuilder()
//        .put(Key.TOPOLOGY_NAME, "test-topology").build();
//    nomadScheduler.initialize(config, runtime);
//    nomadScheduler.onSchedule(null);
//    nomadScheduler.initialize(config, runtime);
//    Thread.sleep(5000);
//    nomadScheduler.onKill(Scheduler.KillTopologyRequest.newBuilder().setTopologyName("test-topology").build());
//    NomadApiClient apiClient = new NomadApiClient(
//        new NomadApiConfiguration.Builder()
//            .setAddress("http://127.0.0.1:4646")
//            .build());
//
//    System.out.println("jobs: " + apiClient.getJobsApi().list());

  }
}
