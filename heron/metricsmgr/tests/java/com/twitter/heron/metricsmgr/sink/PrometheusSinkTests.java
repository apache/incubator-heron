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
package com.twitter.heron.metricsmgr.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.twitter.heron.spi.metricsmgr.metrics.ExceptionInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsInfo;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsRecord;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrometheusSinkTests {

  private static final long NOW = System.currentTimeMillis();

  private final class PrometheusTestSink extends PrometheusSink {

    private PrometheusTestSink() {
    }

    @Override
    protected void startHttpServer(String path, int port) {
      // no need to start the server for tests
    }

    public Map<String, Map<String, Double>> getMetrics() {
      return getMetricsCache().asMap();
    }

    long currentTimeMillis() {
      return NOW;
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
        newRecord("machine/component/instance_1", infos, Collections.emptyList()),
        newRecord("machine/component/instance_2", infos, Collections.emptyList()));
  }

  @Test
  public void testMetricsGrouping() {
    PrometheusTestSink sink = new PrometheusTestSink();
    sink.init(defaultConf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    final Map<String, Map<String, Double>> metrics = sink.getMetrics();
    assertTrue(metrics.containsKey("testTopology/component/instance_1"));
    assertTrue(metrics.containsKey("testTopology/component/instance_2"));
  }

  @Test
  public void testResponse() throws IOException {
    PrometheusTestSink sink = new PrometheusTestSink();
    sink.init(defaultConf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    final String topology = "testTopology";

    //heron_jvm_estimated_usage_par_eden_space_max {toponame="exclamationtopology"
    // component="exclaim1" instance="container_1_exclaim1_1"} 26.0
    final List<String> expectedLines = Arrays.asList(
        createMetric(topology, "component", "instance_1", "metric_1", "1.0"),
        createMetric(topology, "component", "instance_1", "metric_2", "2.0"),
        createMetric(topology, "component", "instance_1", "metric_1", "1.0"),
        createMetric(topology, "component", "instance_1", "metric_2", "2.0")
    );

    final Set<String> generatedLines =
        new HashSet<>(Arrays.asList(new String(sink.generateResponse()).split("\n")));

    assertEquals(expectedLines.size(), generatedLines.size());

    expectedLines.forEach((String line) -> {
      assertTrue(generatedLines.contains(line));
    });
  }

  private String createMetric(String topology, String component, String instance,
        String metric, String value) {
    return String.format("heron_%s{topology=\"%s\",component=\"%s\",instance=\"%s\"} %s %d",
        metric, topology, component, instance, value, NOW);
  }

  private MetricsRecord newRecord(String source, Iterable<MetricsInfo> metrics,
        Iterable<ExceptionInfo> exceptions) {
    return new MetricsRecord(source, metrics, exceptions);
  }
}
