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

package org.apache.heron.metricsmgr;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.testhelpers.CommunicatorTestHelper;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.system.Metrics;
import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;

import static org.apache.heron.common.testhelpers.HeronServerTester.RESPONSE_RECEIVED_TIMEOUT;

/**
 * MetricsManagerServer Tester.
 */
public class MetricsManagerServerTest {
  private static final String METRIC_NAME = "metric-name";
  private static final String METRIC_VALUE = "metric-value";

  private static final int METRICS_COUNT = 20;
  private static final int MESSAGE_SIZE = 10;
  private static final String STACK_TRACE = "stackTrace";
  private static final String LAST_TIME = "lastTime";
  private static final String FIRST_TIME = "firstTime";
  private static final String LOGGING = "logging";
  private static final int EXCEPTION_COUNT = 20;

  private MetricsManagerServer metricsManagerServer;
  private HeronServerTester serverTester;

  @Before
  public void before() throws IOException {
    metricsManagerServer = new MetricsManagerServer(new NIOLooper(), HeronServerTester.SERVER_HOST,
        SysUtils.getFreePort(), HeronServerTester.TEST_SOCKET_OPTIONS, new MultiCountMetric());

    serverTester = new HeronServerTester(metricsManagerServer,
        new MetricsManagerClientRequestHandler(),
        new HeronServerTester.SuccessResponseHandler(Metrics.MetricPublisherRegisterResponse.class,
            new MetricsManagerClientResponseHandler(MESSAGE_SIZE)), RESPONSE_RECEIVED_TIMEOUT);
  }

  @After
  public void after() {
    serverTester.stop();
  }

  /**
   * Method: addSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testAddSinkCommunicator() {
    String name = "test_communicator";
    Communicator<MetricsRecord> sinkCommunicator = new Communicator<>();
    metricsManagerServer.addSinkCommunicator(name, sinkCommunicator);
    Assert.assertTrue(metricsManagerServer.removeSinkCommunicator(name));
  }

  /**
   * Method: removeSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testRemoveSinkCommunicator() {
    String name = "test_communicator";
    Communicator<MetricsRecord> sinkCommunicator = new Communicator<>();
    metricsManagerServer.addSinkCommunicator(name, sinkCommunicator);
    Assert.assertTrue(metricsManagerServer.removeSinkCommunicator(name));
  }

  /**
   * Method: addSinkCommunicator(Communicator&lt;MetricsRecord&gt; communicator)
   */
  @Test
  public void testMetricsManagerServer() throws InterruptedException {
    String name = "test_communicator";
    CountDownLatch offersLatch = new CountDownLatch(MESSAGE_SIZE);
    Communicator<MetricsRecord> sinkCommunicator =
        CommunicatorTestHelper.spyCommunicator(new Communicator<MetricsRecord>(), offersLatch);
    metricsManagerServer.addSinkCommunicator(name, sinkCommunicator);

    serverTester.start();

    HeronServerTester.await(offersLatch);

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
      Assert.assertEquals(METRICS_COUNT, metrics);

      for (ExceptionInfo info : record.getExceptions()) {
        Assert.assertEquals(STACK_TRACE, info.getStackTrace());
        Assert.assertEquals(LAST_TIME, info.getLastTime());
        Assert.assertEquals(FIRST_TIME, info.getFirstTime());
        Assert.assertEquals(LOGGING, info.getLogging());
        Assert.assertEquals(EXCEPTION_COUNT, info.getCount());
        exceptions++;
      }
      Assert.assertEquals(METRICS_COUNT, exceptions);

      messages++;
    }
    Assert.assertEquals(MESSAGE_SIZE, messages);
  }

  private class MetricsManagerClientRequestHandler implements HeronServerTester.TestRequestHandler {

    @Override
    public Message getRequestMessage() {
      Metrics.MetricPublisher publisher = Metrics.MetricPublisher.newBuilder().
          setHostname("hostname").
          setPort(0).
          setComponentName("component").
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

  private class MetricsManagerClientResponseHandler
      implements HeronServerTester.TestResponseHandler {
    private int maxMessages;

    MetricsManagerClientResponseHandler(int maxMessages) {
      this.maxMessages = maxMessages;
    }

    @Override
    public void handleResponse(HeronClient client, StatusCode status,
                               Object ctx, Message response) {
      for (int i = 0; i < maxMessages; i++) {
        sendMessage(client);
      }
    }

    private void sendMessage(HeronClient client) {
      Metrics.MetricPublisherPublishMessage.Builder builder =
          Metrics.MetricPublisherPublishMessage.newBuilder();

      for (int j = 0; j < METRICS_COUNT; j++) {
        builder.addMetrics(
            Metrics.MetricDatum.newBuilder()
                    .setName(METRIC_NAME)
                    .setValue(METRIC_VALUE).build());
      }

      for (int j = 0; j < METRICS_COUNT; j++) {
        builder.addExceptions(
            Metrics.ExceptionData.newBuilder()
                .setStacktrace(STACK_TRACE)
                .setLasttime(LAST_TIME)
                .setFirsttime(FIRST_TIME)
                .setCount(EXCEPTION_COUNT)
                .setLogging(LOGGING).build());
      }
      client.sendMessage(builder.build());
    }
  }
}

