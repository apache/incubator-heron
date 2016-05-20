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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.spi.metricsmgr.metrics.ExceptionInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;

/**
 * MetricsManagerServer Tester.
 */
public class MetricsManagerServerTest {
  private static final String METRIC_NAME = "metric-name";
  private static final String METRIC_VALUE = "metric-value";

  private static final int N = 20;
  private static final int MESSAGE_SIZE = 10;
  private static final String STACK_TRACE = "stackTrace";
  private static final String LAST_TIME = "lastTime";
  private static final String FIRST_TIME = "firstTime";
  private static final String LOGGING = "logging";
  private static final int EXCEPTION_COUNT = 20;

  private static final String SERVER_HOST = "127.0.0.1";
  private static int serverPort;

  private MetricsManagerServer metricsManagerServer;
  private SimpleMetricsClient simpleMetricsClient;
  private NIOLooper serverLooper;

  private ExecutorService threadsPool;

  @Before
  public void before() throws Exception {
    // Get an available port
    serverPort = SysUtils.getFreePort();

    threadsPool = Executors.newFixedThreadPool(2);

    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(100 * 1024 * 1024, 100,
            100 * 1024 * 1024, 100,
            5 * 1024 * 1024,
            5 * 1024 * 1024);

    serverLooper = new NIOLooper();
    metricsManagerServer = new MetricsManagerServer(serverLooper, SERVER_HOST,
        serverPort, serverSocketOptions, new MultiCountMetric());
  }

  @After
  public void after() throws Exception {
    threadsPool.shutdownNow();

    metricsManagerServer.stop();
    metricsManagerServer = null;

    if (simpleMetricsClient != null) {
      simpleMetricsClient.stop();

      simpleMetricsClient.getNIOLooper().exitLoop();
    }

    serverLooper.exitLoop();
    serverLooper = null;

    threadsPool = null;
  }

  /**
   * Method: addSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testAddSinkCommunicator() throws Exception {
    Communicator<MetricsRecord> sinkCommunicator = new Communicator<MetricsRecord>();
    metricsManagerServer.addSinkCommunicator(sinkCommunicator);
    Assert.assertTrue(metricsManagerServer.removeSinkCommunicator(sinkCommunicator));
  }

  /**
   * Method: removeSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testRemoveSinkCommunicator() throws Exception {
    Communicator<MetricsRecord> sinkCommunicator = new Communicator<MetricsRecord>();
    metricsManagerServer.addSinkCommunicator(sinkCommunicator);
    Assert.assertTrue(metricsManagerServer.removeSinkCommunicator(sinkCommunicator));
  }

  /**
   * Method: addSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testMetricsManagerServer() throws Exception {
    final Communicator<MetricsRecord> sinkCommunicator = new Communicator<MetricsRecord>();
    metricsManagerServer.addSinkCommunicator(sinkCommunicator);

    // First run Server
    runServer();

    // Wait a while for server fully starting
    Thread.sleep(3 * 1000);

    // Then run Client
    runClient();


    // Wait some while to let message fully send out
    Thread.sleep(10 * 1000);

    int messages = 0;
    while (!sinkCommunicator.isEmpty()) {
      int exceptions = 0;
      int metrics = 0;

      MetricsRecord record = sinkCommunicator.poll();

      Assert.assertEquals("hostname:0/component/instance-id", record.getSource());
      Assert.assertEquals("default", record.getContext());

      for (MetricsInfo info : record.getMetrics()) {
        Assert.assertEquals(METRIC_NAME, info.getName());
        Assert.assertEquals(METRIC_VALUE, info.getValue());
        metrics++;
      }
      Assert.assertEquals(N, metrics);

      for (ExceptionInfo info : record.getExceptions()) {
        Assert.assertEquals(STACK_TRACE, info.getStackTrace());
        Assert.assertEquals(LAST_TIME, info.getLastTime());
        Assert.assertEquals(FIRST_TIME, info.getFirstTime());
        Assert.assertEquals(LOGGING, info.getLogging());
        Assert.assertEquals(EXCEPTION_COUNT, info.getCount());
        exceptions++;
      }
      Assert.assertEquals(N, exceptions);

      messages++;
    }
    Assert.assertEquals(MESSAGE_SIZE, messages);
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
      @Override
      public void run() {
        try {
          NIOLooper looper = new NIOLooper();
          simpleMetricsClient =
              new SimpleMetricsClient(looper, SERVER_HOST, serverPort, MESSAGE_SIZE);
          simpleMetricsClient.start();
          looper.loop();
        } catch (IOException e) {
          throw new RuntimeException("Some error instantiating client");
        } finally {
          simpleMetricsClient.stop();
        }
      }
    };
    threadsPool.execute(runClient);
  }

  private static class SimpleMetricsClient extends HeronClient {
    private static final Logger LOG = Logger.getLogger(SimpleMetricsClient.class.getName());
    private int maxMessages;

    SimpleMetricsClient(NIOLooper looper, String host, int port, int maxMessages) {
      super(looper, host, port,
          new HeronSocketOptions(100 * 1024 * 1024, 100,
              100 * 1024 * 1024, 100,
              5 * 1024 * 1024,
              5 * 1024 * 1024));
      this.maxMessages = maxMessages;
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
          setComponentName("component").
          setInstanceId("instance-id").
          setInstanceIndex(1).
          build();
      Metrics.MetricPublisherRegisterRequest request =
          Metrics.MetricPublisherRegisterRequest.newBuilder().setPublisher(publisher).build();

      sendRequest(request, Metrics.MetricPublisherRegisterResponse.newBuilder());

    }

    private void sendMessage() {
      Metrics.MetricPublisherPublishMessage.Builder builder =
          Metrics.MetricPublisherPublishMessage.newBuilder();

      for (int j = 0; j < N; j++) {
        Metrics.MetricDatum metricDatum =
            Metrics.MetricDatum.newBuilder().setName(METRIC_NAME).setValue(METRIC_VALUE).build();
        builder.addMetrics(metricDatum);
      }

      for (int j = 0; j < N; j++) {
        Metrics.ExceptionData exceptionData = Metrics.ExceptionData.newBuilder().
            setStacktrace(STACK_TRACE).setLasttime(LAST_TIME).setFirsttime(FIRST_TIME).
            setCount(EXCEPTION_COUNT).setLogging(LOGGING).build();
        builder.addExceptions(exceptionData);
      }
      sendMessage(builder.build());
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      if (response instanceof Metrics.MetricPublisherRegisterResponse) {
        Assert.assertEquals(Common.StatusCode.OK,
            ((Metrics.MetricPublisherRegisterResponse) response).getStatus().getStatus());

        for (int i = 0; i < maxMessages; i++) {
          sendMessage();
        }

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

