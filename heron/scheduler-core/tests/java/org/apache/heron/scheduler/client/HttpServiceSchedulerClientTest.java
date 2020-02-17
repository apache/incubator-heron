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

package org.apache.heron.scheduler.client;

import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.proto.scheduler.Scheduler;
import org.apache.heron.scheduler.Command;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.utils.NetworkUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(NetworkUtils.class)
public class HttpServiceSchedulerClientTest {
  private static final String TOPOLOGY_NAME = "topologyName";
  private static final String SCHEDULER_HTTP_ENDPOINT = "";
  private static final String COMMAND_ENDPOINT = "http://endpoint/command";

  private final Config config = Mockito.mock(Config.class);
  private final Config runtime = Mockito.mock(Config.class);

  private Scheduler.RestartTopologyRequest restartTopologyRequest =
      Scheduler.RestartTopologyRequest.newBuilder().
          setTopologyName(TOPOLOGY_NAME).
          setContainerIndex(-1).
          build();
  private Scheduler.KillTopologyRequest killTopologyRequest =
      Scheduler.KillTopologyRequest.newBuilder().setTopologyName(TOPOLOGY_NAME).build();

  @Test
  public void testRestartTopology() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));
    Mockito.doReturn(true).
        when(client).
        requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Assert.assertTrue(client.restartTopology(restartTopologyRequest));
    Mockito.verify(client).
        requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Mockito.doReturn(false).
        when(client).
        requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Assert.assertFalse(client.restartTopology(restartTopologyRequest));
    Mockito.verify(client, Mockito.times(2)).
        requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());
  }

  @Test
  public void testKillTopology() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));
    Mockito.doReturn(true).
        when(client).
        requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Assert.assertTrue(client.killTopology(killTopologyRequest));
    Mockito.verify(client).
        requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Mockito.doReturn(false).
        when(client).requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Assert.assertFalse(client.killTopology(killTopologyRequest));
    Mockito.verify(client, Mockito.times(2)).
        requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());
  }

  /**
   * Test Request Scheduler Service
   */
  @Test
  public void testRequestSchedulerService() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));

    // Failed to create new http connection
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(null).
        when(NetworkUtils.class, "getHttpConnection", Mockito.any(String.class));
    Mockito.doReturn(COMMAND_ENDPOINT).
        when(client).getCommandEndpoint(Mockito.any(String.class), Mockito.any(Command.class));
    Assert.assertFalse(
        client.requestSchedulerService(
            Mockito.any(Command.class), Mockito.any(byte[].class)));

    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
    PowerMockito.doReturn(connection).
        when(NetworkUtils.class, "getHttpConnection", Mockito.any(String.class));

    // Failed to send http post request
    PowerMockito.doReturn(false).
        when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.eq(connection), Mockito.any(String.class), Mockito.any(byte[].class));
    Assert.assertFalse(
        client.requestSchedulerService(
            Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection).disconnect();

    // Received non-ok response
    PowerMockito.doReturn(true).
        when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.eq(connection), Mockito.any(String.class), Mockito.any(byte[].class));
    Scheduler.SchedulerResponse notOKResponse = SchedulerUtils.constructSchedulerResponse(false);
    PowerMockito.doReturn(notOKResponse.toByteArray()).
        when(NetworkUtils.class, "readHttpResponse", Mockito.eq(connection));
    Assert.assertFalse(
        client.requestSchedulerService(
            Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection, Mockito.times(2)).disconnect();

    // Received ok response -- success case
    Scheduler.SchedulerResponse oKResponse = SchedulerUtils.constructSchedulerResponse(true);
    PowerMockito.doReturn(oKResponse.toByteArray()).
        when(NetworkUtils.class, "readHttpResponse", Mockito.eq(connection));
    Assert.assertTrue(
        client.requestSchedulerService(
            Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection, Mockito.times(3)).disconnect();
  }
}
