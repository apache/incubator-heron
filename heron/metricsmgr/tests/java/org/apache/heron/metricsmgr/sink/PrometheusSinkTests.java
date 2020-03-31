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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

    /*
    # example: metrics.yaml
    rules:
      - pattern: kafka.(\w+)<type=(.+), name=(.+)PerSec\w*, (.+)=(.+)><>Count
        name: kafka_$1_$2_$3_total
        attrNameSnakeCase: true
        type: COUNTER
        labels:
          "$4": "$5"
        type: COUNTER
    */
    /*
    example: metrics
      kafkaOffset/nginx-lfp-beacon/totalSpoutLag
      kafkaOffset/lads_event_meta_backfill_data/partition_10/spoutLag
     */
    List<Map<String, Object>> rules = Lists.newArrayList();
    defaultConf.put("rules", rules);
    Map<String, Object> rule1 = Maps.newHashMap();
    Map<String, Object> labels1 = Maps.newHashMap();
    rules.add(rule1);
    rule1.put("pattern", "kafkaOffset/(.+)/(.+)");
    rule1.put("name", "kafka_offset_$2");
    rule1.put("type", "COUNTER");
    rule1.put("attrNameSnakeCase", true);
    rule1.put("labels", labels1);
    labels1.put("topic", "$1");

    Map<String, Object> rule2 = Maps.newHashMap();
    Map<String, Object> labels2 = Maps.newHashMap();
    rules.add(rule2);
    rule2.put("pattern", "kafkaOffset/(.+)/partition_(\\d+)/(.+)");
    rule2.put("name", "kafka_offset_partition_$3");
    rule2.put("type", "COUNTER");
    rule2.put("labels", labels2);
    rule2.put("attrNameSnakeCase", true);
    labels2.put("topic", "$1");
    labels2.put("partition", "$2");

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

  @Test
  public void testResponseWhenMetricNamesHaveAnInstanceId() throws IOException {
    Iterable<MetricsInfo> infos = Arrays.asList(
        new MetricsInfo("__connection_buffer_by_instanceid/container_1_word_5/packets", "1.0"),
        new MetricsInfo("__time_spent_back_pressure_by_compid/container_1_exclaim1_1", "1.0"),
        new MetricsInfo("__client_stmgr-92/__ack_tuples_to_stmgrs", "1.0"),
        new MetricsInfo("__instance_bytes_received/1", "1.0")
    );

    records = Arrays.asList(
        newRecord("machine/__stmgr__/stmgr-1", infos, Collections.emptyList())
    );

    PrometheusTestSink sink = new PrometheusTestSink();
    sink.init(defaultConf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    final String topology = "testTopology";

    final List<String> expectedLines = Arrays.asList(
        createMetric(topology, "__stmgr__", "stmgr-1",
            "connection_buffer_by_instanceid_packets",
            "container_1_word_5", "1.0"),
        createMetric(topology, "__stmgr__", "stmgr-1",
            "time_spent_back_pressure_by_compid",
            "container_1_exclaim1_1", "1.0"),
        createMetric(topology, "__stmgr__", "stmgr-1",
            "client_stmgr_ack_tuples_to_stmgrs", "stmgr-92", "1.0"),
        createMetric(topology, "__stmgr__", "stmgr-1",
            "instance_bytes_received", "1", "1.0")
    );

    final Set<String> generatedLines =
        new HashSet<>(Arrays.asList(new String(sink.generateResponse()).split("\n")));

    assertEquals(expectedLines.size(), generatedLines.size());

    expectedLines.forEach((String line) -> {
      assertTrue(generatedLines.contains(line));
    });
  }

  @Test
  public void testApacheStormKafkaMetrics() throws IOException {
    Iterable<MetricsInfo> infos = Arrays.asList(
        new MetricsInfo("kafkaOffset/event_data/partition_0/spoutLag", "1.0"),
        new MetricsInfo("kafkaOffset/event_data/partition_10/spoutLag", "1.0"),
        new MetricsInfo("kafkaOffset/event_data/partition_0/earliestTimeOffset", "1.0"),
        new MetricsInfo("kafkaOffset/event_data/totalRecordsInPartitions", "1.0"),
        new MetricsInfo("kafkaOffset/event_data/totalSpoutLag", "1.0"),
        new MetricsInfo("kafkaOffset/event_data/partition_2/spoutLag", "1.0")
    );

    records = Arrays.asList(
        newRecord("shared-aurora-036:31/spout-release-1/container_1_spout-release-1_31",
            infos, Collections.emptyList())
    );
    PrometheusTestSink sink = new PrometheusTestSink();
    sink.init(defaultConf, context);
    for (MetricsRecord r : records) {
      sink.processRecord(r);
    }

    final String topology = "testTopology";

    final List<String> expectedLines = Arrays.asList(
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_partition_spout_lag", "event_data", "0", "1.0"),
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_partition_spout_lag", "event_data", "10", "1.0"),
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_partition_earliest_time_offset", "event_data", "0", "1.0"),
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_total_records_in_partitions", "event_data", null, "1.0"),
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_total_spout_lag", "event_data", null, "1.0"),
        createOffsetMetric(topology, "spout-release-1", "container_1_spout-release-1_31",
            "kafka_offset_partition_spout_lag", "event_data", "2", "1.0")
    );

    final Set<String> generatedLines =
        new HashSet<>(Arrays.asList(new String(sink.generateResponse()).split("\n")));

    assertEquals(expectedLines.size(), generatedLines.size());

    expectedLines.forEach((String line) -> {
      assertTrue(generatedLines.contains(line));
    });
  }

  @Test
  public void testComponentType() {
    Map<String, Double> metrics = new HashMap<>();
    metrics.put("__execute-time-ns/default", 1d);
    assertEquals("bolt", PrometheusSink.getComponentType(metrics));

    metrics = new HashMap<>();
    metrics.put("__execute-time-ns/stream1", 1d);
    assertEquals("bolt", PrometheusSink.getComponentType(metrics));

    metrics = new HashMap<>();
    metrics.put("__next-tuple-count", 1d);
    assertEquals("spout", PrometheusSink.getComponentType(metrics));

    metrics = new HashMap<>();
    assertNull(PrometheusSink.getComponentType(metrics));
  }

  private String createMetric(String topology, String component, String instance,
        String metric, String value) {
    return createMetric(topology, component, instance, metric, null, value);
  }

  private String createMetric(String topology, String component, String instance,
        String metric, String metricNameInstanceId, String value) {

    if (metricNameInstanceId != null) {
      return String.format("heron_%s"
              + "{component=\"%s\",instance_id=\"%s\",metric_instance_id=\"%s\",topology=\"%s\"}"
              + " %s %d",
          metric, component, instance, metricNameInstanceId, topology, value, NOW);
    } else {
      return String.format("heron_%s{component=\"%s\",instance_id=\"%s\",topology=\"%s\"} %s %d",
          metric, component, instance, topology, value, NOW);
    }
  }

  private String createOffsetMetric(String topology, String component, String instance,
                              String metric, String topic, String partition, String value) {

    if (partition != null) {
      return String.format("heron_%s"
              + "{component=\"%s\",instance_id=\"%s\",partition=\"%s\","
              + "topic=\"%s\",topology=\"%s\"}"
              + " %s %d",
          metric, component, instance, partition, topic, topology, value, NOW);
    } else {
      return String.format("heron_%s"
              + "{component=\"%s\",instance_id=\"%s\",topic=\"%s\",topology=\"%s\"} %s %d",
          metric, component, instance, topic, topology, value, NOW);
    }
  }

  private MetricsRecord newRecord(String source, Iterable<MetricsInfo> metrics,
        Iterable<ExceptionInfo> exceptions) {
    return new MetricsRecord(source, metrics, exceptions);
  }
}
