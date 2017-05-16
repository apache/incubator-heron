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

package com.twitter.heron.metricsmgr;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.tmaster.TopologyMaster;

/**
 * Test whether MetricsManagerServer could handle TMasterLocationRefreshMessage correctly.
 * <p>
 * We make a SimpleTMasterLocationPublisher, which would send two TMasterLocationRefreshMessage,
 * (twice each) after connected and registered with
 * MetricsManagerServer, and then we check:
 * 1. Whether onMessage(...) is invoked 4 times, with correct arguments.
 * <p>
 * 2. Whether onMessage(...) is invoked 4 times, with correct order.
 * <p>
 * 3. Whether eventually the TMasterLocation in SingletonRegistry should be the latest one.
 */

public class HandleTMasterLocationTest {

  private static final HeronSocketOptions TEST_SOCKET_OPTIONS = new HeronSocketOptions(
      ByteAmount.fromMegabytes(100), 100,
      ByteAmount.fromMegabytes(100), 100,
      ByteAmount.fromMegabytes(5),
      ByteAmount.fromMegabytes(5));

  // Two TMasterLocationRefreshMessage to verify
  private static final Metrics.TMasterLocationRefreshMessage TMASTERLOCATIONREFRESHMESSAGE0 =
      Metrics.TMasterLocationRefreshMessage.newBuilder().setTmaster(
          TopologyMaster.TMasterLocation.newBuilder().
              setTopologyName("topology-name").setTopologyId("topology-id").
              setHost("host").setControllerPort(0).setMasterPort(0)).
          build();

  private static final Metrics.TMasterLocationRefreshMessage TMASTERLOCATIONREFRESHMESSAGE1 =
      Metrics.TMasterLocationRefreshMessage.newBuilder().setTmaster(
          TopologyMaster.TMasterLocation.newBuilder().
              setTopologyName("topology-name").setTopologyId("topology-id").
              setHost("host").setControllerPort(0).setMasterPort(1)).
          build();

  // Bean name to register the TMasterLocation object into SingletonRegistry
  private static final String TMASTER_LOCATION_BEAN_NAME =
      TopologyMaster.TMasterLocation.newBuilder().getDescriptorForType().getFullName();

  private static final String SERVER_HOST = "127.0.0.1";
  private static int serverPort;

  private MetricsManagerServer metricsManagerServer;
  private SimpleTMasterLocationPublisher simpleLocationPublisher;
  private NIOLooper serverLooper;

  private ExecutorService threadsPool;

  @Before
  public void before() throws Exception {
    // Get an available port
    serverPort = SysUtils.getFreePort();

    threadsPool = Executors.newFixedThreadPool(2);

    serverLooper = new NIOLooper();

    // Spy it for unit test
    metricsManagerServer =
        Mockito.spy(new MetricsManagerServer(serverLooper, SERVER_HOST,
            serverPort, TEST_SOCKET_OPTIONS, new MultiCountMetric()));
  }

  @After
  @SuppressWarnings("unchecked")
  public void after() throws Exception {
    threadsPool.shutdownNow();

    metricsManagerServer.stop();
    metricsManagerServer = null;

    simpleLocationPublisher.stop();

    simpleLocationPublisher.getNIOLooper().exitLoop();
    serverLooper.exitLoop();
    serverLooper = null;

    threadsPool = null;

    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  @Test
  public void testHandleTMasterLocation() throws Exception {
    // First run Server
    runServer();

    // Wait a while for server fully starting
    Thread.sleep(3 * 1000);

    // Then run Client
    runClient();


    // Wait some while to let message fully send out
    Thread.sleep(10 * 1000);

    // Verification
    TopologyMaster.TMasterLocation tMasterLocation = (TopologyMaster.TMasterLocation)
        SingletonRegistry.INSTANCE.getSingleton(TMASTER_LOCATION_BEAN_NAME);

    // Verify we received these message
    Mockito.verify(metricsManagerServer, Mockito.times(2)).
        onMessage(Mockito.any(SocketChannel.class), Mockito.eq(TMASTERLOCATIONREFRESHMESSAGE0));
    Mockito.verify(metricsManagerServer, Mockito.times(2)).
        onMessage(Mockito.any(SocketChannel.class), Mockito.eq(TMASTERLOCATIONREFRESHMESSAGE1));

    // Verify we received message in order
    InOrder inOrder = Mockito.inOrder(metricsManagerServer);

    inOrder.verify(metricsManagerServer, Mockito.times(2)).
        onMessage(Mockito.any(SocketChannel.class), Mockito.eq(TMASTERLOCATIONREFRESHMESSAGE0));
    inOrder.verify(metricsManagerServer, Mockito.times(2)).
        onMessage(Mockito.any(SocketChannel.class), Mockito.eq(TMASTERLOCATIONREFRESHMESSAGE1));

    Assert.assertEquals("topology-name", tMasterLocation.getTopologyName());
    Assert.assertEquals("topology-id", tMasterLocation.getTopologyId());
    Assert.assertEquals("host", tMasterLocation.getHost());
    Assert.assertEquals(0, tMasterLocation.getControllerPort());
    Assert.assertEquals(1, tMasterLocation.getMasterPort());
  }

  private void runServer() {

    Runnable runServer = new Runnable() {
      @Override
      public void run() {
        metricsManagerServer.start();
        metricsManagerServer.getNIOLooper().loop();
      }
    };
    threadsPool.execute(runServer);
  }

  private void runClient() {

    Runnable runClient = new Runnable() {
      private NIOLooper looper;

      @Override
      public void run() {
        try {
          looper = new NIOLooper();
          simpleLocationPublisher =
              new SimpleTMasterLocationPublisher(looper, SERVER_HOST, serverPort);
          simpleLocationPublisher.start();
          looper.loop();

        } catch (IOException e) {
          throw new RuntimeException("Some error instantiating client");
        } finally {
          simpleLocationPublisher.stop();
          if (looper != null) {
            looper.exitLoop();
          }
        }
      }
    };
    threadsPool.execute(runClient);
  }

  private static class SimpleTMasterLocationPublisher extends HeronClient {
    private static final Logger LOG = Logger.getLogger(
        SimpleTMasterLocationPublisher.class.getName());

    SimpleTMasterLocationPublisher(NIOLooper looper, String host, int port) {
      super(looper, host, port, TEST_SOCKET_OPTIONS);
    }

    @Override
    public void onConnect(StatusCode status) {
      if (status != StatusCode.OK) {
        org.junit.Assert.fail("Connection with server failed");
      } else {
        LOG.info("Connected with Metrics Manager Server");
        sendRequest();
      }
    }

    @Override
    public void onError() {
      org.junit.Assert.fail("Error in client while talking to server");
    }

    @Override
    public void onClose() {

    }

    private void sendRequest() {
      Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().
          setHostname("hostname").
          setPort(0).
          setComponentName("tmaster-location-publisher").
          setInstanceId("instance-id").
          setInstanceIndex(1).
          build();

      Metrics.MetricPublisherRegisterRequest request =
          Metrics.MetricPublisherRegisterRequest.newBuilder().setPublisher(publisher).build();

      sendRequest(request, Metrics.MetricPublisherRegisterResponse.newBuilder());
    }

    // We send two TMasterLocationRefreshMessage twice each
    // Then we check:
    // 1. Whether onMessage(...) is invoked 4 times, with correct arguments.
    // 2. Finally the TMasterLocation in SingletonRegistry should be the latest one.
    private void sendMessage() {
      // First send TMASTERLOCATIONREFRESHMESSAGE0 twice
      sendMessage(TMASTERLOCATIONREFRESHMESSAGE0);
      sendMessage(TMASTERLOCATIONREFRESHMESSAGE0);

      // Then send TMASTERLOCATIONREFRESHMESSAGE1 twice
      sendMessage(TMASTERLOCATIONREFRESHMESSAGE1);
      sendMessage(TMASTERLOCATIONREFRESHMESSAGE1);
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      if (response instanceof Metrics.MetricPublisherRegisterResponse) {
        Assert.assertEquals(Common.StatusCode.OK,
            ((Metrics.MetricPublisherRegisterResponse) response).getStatus().getStatus());

        // Start sending the TMasterLocation
        sendMessage();

      } else {
        org.junit.Assert.fail("Unknown type of response received");
      }
    }

    @Override
    public void onIncomingMessage(Message request) {
      org.junit.Assert.fail("Expected message from client");
    }
  }
}
