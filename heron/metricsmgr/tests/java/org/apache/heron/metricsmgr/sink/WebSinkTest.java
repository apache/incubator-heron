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

package org.apache.heron.metricsmgr.sink;


import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Ticker;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

/**
 * WebSink Tester.
 */
public class WebSinkTest {

  private final class WebTestSink extends WebSink {
    private String servicePath;
    private int servicePort;

    private WebTestSink() {
    }

    private WebTestSink(Ticker cacheTicker) {
      super(cacheTicker);
    }

    @Override
    protected void startHttpServer(String path, int port) {
      this.servicePath = path;
      this.servicePort = port;
    }

    public Map<String, Object> getMetrics() {
      return getMetricsCache().asMap();
    }

    void syncCache() {
      getMetricsCache().cleanUp();
    }
  }

  private Map<String, Object> defaultConf;
  private SinkContext context;
  private List<MetricsRecord> records;


  @Before
  public void before() throws IOException {

    defaultConf = new HashMap<>();
    defaultConf.put("port", "9999");
    defaultConf.put("path", "test");
    defaultConf.put("flat-metrics", "true");
    defaultConf.put("include-topology-name", "false");

    context = Mockito.mock(SinkContext.class);
    Mockito.when(context.getTopologyName()).thenReturn("testTopology");
    Mockito.when(context.getSinkId()).thenReturn("testId");

    Iterable<MetricsInfo> infos = Arrays.asList(new MetricsInfo("metric_1", "1.0"),
        new MetricsInfo("metric_2", "2.0"));

    records = Arrays.asList(
        new MetricsRecord("machine/stuff/record_1", infos, Collections.<ExceptionInfo>emptyList()),
        new MetricsRecord("record_2", infos, Collections.<ExceptionInfo>emptyList()));
  }

  /**
   * Testing exception when illegal port is specified
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalPort() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.put("port", "fdfsaf");
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
  }

  /**
   * Testing exception when no port is specified
   */
  @Test(expected = IllegalArgumentException.class)
  public void testNoPort() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.remove("port");
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
  }

  /**
   * Testing port and path setting
   */
  @Test
  public void testPortAndPath() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
    Assert.assertEquals(sink.servicePort, 9999);
    Assert.assertEquals(sink.servicePath, "test");
  }

  /**
   * Testing flat map with metrics
   */
  @Test
  public void testFlatMetrics() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    Map<String, Object> results = sink.getMetrics();
    Assert.assertEquals(4, results.size());
    Assert.assertEquals(results.get("/stuff/record_1/metric_1"), 1.0d);
    Assert.assertEquals(results.get("/stuff/record_1/metric_2"), 2.0d);
    Assert.assertEquals(results.get("/record_2/metric_1"), 1.0d);
    Assert.assertEquals(results.get("/record_2/metric_2"), 2.0d);
  }

  /**
   * Testing flat map with metrics, prefixed with topology name
   */
  @Test
  public void testIncludeTopologyName() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.put("include-topology-name", "true");
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    Map<String, Object> results = sink.getMetrics();
    Assert.assertEquals(4, results.size());
    Assert.assertEquals(results.get("testTopology/stuff/record_1/metric_1"), 1.0d);
    Assert.assertEquals(results.get("testTopology/stuff/record_1/metric_2"), 2.0d);
    Assert.assertEquals(results.get("testTopology/record_2/metric_1"), 1.0d);
    Assert.assertEquals(results.get("testTopology/record_2/metric_2"), 2.0d);
  }

  /**
   * Testing grouped map with metrics
   */
  @Test
  public void testGroupedMetrics() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.put("flat-metrics", "false");
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    //Update and override MetricsRecord 1
    Iterable<MetricsInfo> infos2 = Arrays.asList(new MetricsInfo("metric_1", "3.0"),
        new MetricsInfo("metric_3", "1.0"));
    sink.processRecord(new MetricsRecord(records.get(0).getSource(),
        infos2,
        Collections.<ExceptionInfo>emptyList()));

    Map<String, Object> results = sink.getMetrics();
    Assert.assertEquals(2, results.size());
    @SuppressWarnings("unchecked")
    Map<String, Object> record1 = (Map<String, Object>) results.get("/stuff/record_1");
    @SuppressWarnings("unchecked")
    Map<String, Object> record2 = (Map<String, Object>) results.get("/record_2");

    Assert.assertEquals(record1.get("metric_1"), 3.0d);
    Assert.assertEquals(record1.get("metric_2"), 2.0d);
    Assert.assertEquals(record1.get("metric_3"), 1.0d);
    Assert.assertEquals(record2.get("metric_1"), 1.0d);
    Assert.assertEquals(record2.get("metric_2"), 2.0d);
  }


  /**
   * Testing max metrics size, and oldest keys get expired
   */
  @Test
  public void testMaxMetrics() {
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.put("metrics-cache-max-size", "2");
    WebTestSink sink = new WebTestSink();
    sink.init(conf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    Map<String, Object> results = sink.getMetrics();
    Assert.assertEquals(2, results.size());
    Assert.assertEquals(results.get("/record_2/metric_1"), 1.0d);
    Assert.assertEquals(results.get("/record_2/metric_2"), 2.0d);
  }

  /**
   * Testing TTL
   */
  @Test
  public void testTTLMetrics() throws InterruptedException {
    Duration cacheTTL = Duration.ofSeconds(1);
    Map<String, Object> conf = new HashMap<>(defaultConf);
    conf.put("metrics-cache-ttl-sec", cacheTTL.getSeconds());

    FakeTicker ticker = new FakeTicker();
    WebTestSink sink = new WebTestSink(ticker);
    sink.init(conf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    Assert.assertEquals(records.size() * 2, sink.getMetrics().size());
    ticker.advance(cacheTTL.plusMillis(1));
    sink.syncCache();

    Assert.assertEquals(0, sink.getMetrics().size());
  }

  private class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong();

    void advance(Duration duration) {
      nanos.addAndGet(duration.toNanos());
    }

    @Override
    public long read() {
      return nanos.getAndAdd(0);
    }
  }
}
