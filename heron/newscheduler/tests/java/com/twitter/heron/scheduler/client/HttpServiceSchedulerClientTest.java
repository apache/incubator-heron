package com.twitter.heron.scheduler.client;

import java.net.HttpURLConnection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.scheduler.Command;
import com.twitter.heron.spi.utils.NetworkUtils;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HttpUtils.class})
public class HttpServiceSchedulerClientTest {
  private final String TOPOLOGY_NAME = "topologyName";
  private final String SCHEDULER_HTTP_ENDPOINT = "";

  private final Config config = Mockito.mock(Config.class);
  private final Config runtime = Mockito.mock(Config.class);

  Scheduler.RestartTopologyRequest restartTopologyRequest =
      Scheduler.RestartTopologyRequest.newBuilder().
          setTopologyName(TOPOLOGY_NAME).
          setContainerIndex(-1).
          build();
  Scheduler.KillTopologyRequest killTopologyRequest =
      Scheduler.KillTopologyRequest.newBuilder().setTopologyName(TOPOLOGY_NAME).build();

  @Test
  public void testRestartTopology() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));
    Mockito.doReturn(true).when(client).requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Assert.assertTrue(client.restartTopology(restartTopologyRequest));
    Mockito.verify(client).requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Mockito.doReturn(false).when(client).requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());

    Assert.assertFalse(client.restartTopology(restartTopologyRequest));
    Mockito.verify(client, Mockito.times(2)).requestSchedulerService(Command.RESTART, restartTopologyRequest.toByteArray());
  }

  @Test
  public void testKillTopology() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));
    Mockito.doReturn(true).when(client).requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Assert.assertTrue(client.killTopology(killTopologyRequest));
    Mockito.verify(client).requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Mockito.doReturn(false).when(client).requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());

    Assert.assertFalse(client.killTopology(killTopologyRequest));
    Mockito.verify(client, Mockito.times(2)).requestSchedulerService(Command.KILL, killTopologyRequest.toByteArray());
  }

  @Test
  public void testRequestSchedulerService() throws Exception {
    HttpServiceSchedulerClient client =
        Mockito.spy(new HttpServiceSchedulerClient(config, runtime, SCHEDULER_HTTP_ENDPOINT));

    // Failed to create new http connection
    Mockito.doReturn(null).when(client).createHttpConnection(Mockito.any(Command.class));
    Assert.assertFalse(client.requestSchedulerService(Mockito.any(Command.class), Mockito.any(byte[].class)));

    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);
    Mockito.doReturn(connection).when(client).createHttpConnection(Mockito.any(Command.class));

    // Failed to send http post request
    PowerMockito.spy(HttpUtils.class);
    PowerMockito.doReturn(false).when(HttpUtils.class, "sendHttpPostRequest", Mockito.eq(connection), Mockito.any(byte[].class));
    Assert.assertFalse(client.requestSchedulerService(Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection).disconnect();

    // Received non-ok response
    PowerMockito.doReturn(true).when(HttpUtils.class, "sendHttpPostRequest", Mockito.eq(connection), Mockito.any(byte[].class));
    Scheduler.SchedulerResponse notOKResponse =
        Scheduler.SchedulerResponse.newBuilder().setStatus(NetworkUtils.getHeronStatus(false)).build();
    PowerMockito.doReturn(notOKResponse.toByteArray()).when(HttpUtils.class, "readHttpResponse", Mockito.eq(connection));
    Assert.assertFalse(client.requestSchedulerService(Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection, Mockito.times(2)).disconnect();

    // Received ok response -- success case
    Scheduler.SchedulerResponse oKResponse =
        Scheduler.SchedulerResponse.newBuilder().setStatus(NetworkUtils.getHeronStatus(true)).build();
    PowerMockito.doReturn(oKResponse.toByteArray()).when(HttpUtils.class, "readHttpResponse", Mockito.eq(connection));
    Assert.assertTrue(client.requestSchedulerService(Mockito.any(Command.class), Mockito.any(byte[].class)));
    Mockito.verify(connection, Mockito.times(3)).disconnect();
  }
}
