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
import java.time.Duration;
import java.util.Map;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.common.testhelpers.HeronServerTester;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.tmaster.TopologyMaster;

import static com.twitter.heron.common.testhelpers.HeronServerTester.RESPONSE_RECEIVED_TIMEOUT;
import static org.mockito.Mockito.spy;

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

  private LatchedMultiCountMetric serverMetrics;
  private MetricsManagerServer metricsManagerServer;
  private HeronServerTester heronServerTester;

  @Before
  public void before() throws IOException {
    // MetricsManagerServer increments this counter every time a location refresh message is
    // received, so we can await this counter getting to 4 before proceeding with the test
    serverMetrics = new LatchedMultiCountMetric("tmaster-location-received", 4L);

    // Spy it for unit test
    metricsManagerServer =
        spy(new MetricsManagerServer(new NIOLooper(), HeronServerTester.SERVER_HOST,
            SysUtils.getFreePort(), HeronServerTester.TEST_SOCKET_OPTIONS, serverMetrics));

    heronServerTester = new HeronServerTester(metricsManagerServer,
        new TestRequestHandler(),
        new HeronServerTester.SuccessResponseHandler(
            Metrics.MetricPublisherRegisterResponse.class, new TestResponseHandler()),
        RESPONSE_RECEIVED_TIMEOUT);
    heronServerTester.start();
  }

  @After
  @SuppressWarnings("unchecked")
  public void after() throws NoSuchFieldException, IllegalAccessException {
    heronServerTester.stop();

    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  @Test
  public void testHandleTMasterLocation() throws InterruptedException {
    serverMetrics.await(Duration.ofSeconds(10));

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

  private static final class TestRequestHandler implements HeronServerTester.TestRequestHandler {
    @Override
    public Message getRequestMessage() {
      Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().
          setHostname("hostname").
          setPort(0).
          setComponentName("tmaster-location-publisher").
          setInstanceId("instance-id").
          setInstanceIndex(1).
          build();

      return Metrics.MetricPublisherRegisterRequest.newBuilder().setPublisher(publisher).build();
    }

    @Override
    public Message.Builder getResponseBuilder() {
      return Metrics.MetricPublisherRegisterResponse.newBuilder();
    }
  }

  private static final class TestResponseHandler implements HeronServerTester.TestResponseHandler {
    @Override
    public void handleResponse(HeronClient client, StatusCode status,
                               Object ctx, Message response) {
      // We send two TMasterLocationRefreshMessage twice each
      // Then we check:
      // 1. Whether onMessage(...) is invoked 4 times, with correct arguments.
      // 2. Finally the TMasterLocation in SingletonRegistry should be the latest one.
      // First send TMASTERLOCATIONREFRESHMESSAGE0 twice
      client.sendMessage(TMASTERLOCATIONREFRESHMESSAGE0);
      client.sendMessage(TMASTERLOCATIONREFRESHMESSAGE0);

      // Then send TMASTERLOCATIONREFRESHMESSAGE1 twice
      client.sendMessage(TMASTERLOCATIONREFRESHMESSAGE1);
      client.sendMessage(TMASTERLOCATIONREFRESHMESSAGE1);
    }
  }
}
