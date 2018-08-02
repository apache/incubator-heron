//  Licensed to the Apache Software Foundation (ASF) under one
//  or more contributor license agreements.  See the NOTICE file
//  distributed with this work for additional information
//  regarding copyright ownership.  The ASF licenses this file
//  to you under the Apache License, Version 2.0 (the
//  "License"); you may not use this file except in compliance
//  with the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an
//  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
//  KIND, either express or implied.  See the License for the
//  specific language governing permissions and limitations
//  under the License.
package org.apache.heron.metricsmgr.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

public class InfluxSinkTests {

  private Map<String, Object> defaultConf;
  private SinkContext context;
  private List<MetricsRecord> records;

  private MetricsRecord newRecord(String source, Iterable<MetricsInfo> metrics,
                                  Iterable<ExceptionInfo> exceptions) {
    return new MetricsRecord(source, metrics, exceptions);
  }

  @Before
  public void before() throws IOException {

    defaultConf = new HashMap<>();
    defaultConf.put(InfluxDBSink.SERVER_PORT_KEY, 8086);
    defaultConf.put(InfluxDBSink.SERVER_HOST_KEY, "http://localhost");
    defaultConf.put(InfluxDBSink.METRIC_DB_NAME_KEY, "heron");

    context = Mockito.mock(SinkContext.class);
    Mockito.when(context.getTopologyName()).thenReturn("testTopology");
    Mockito.when(context.getSinkId()).thenReturn("testId");

    Iterable<MetricsInfo> infos = Arrays.asList(new MetricsInfo("test/metric_1", "1.0"),
        new MetricsInfo("test/metric_2", "2.0"));

    records = Arrays.asList(
        newRecord("machine/component/instance_1", infos, Collections.emptyList()),
        newRecord("machine/component/instance_2", infos, Collections.emptyList()));
  }

  @Test
  public void testCreateWithAuth() {
    InfluxDBSink influx = new InfluxDBSink();

    defaultConf.put(InfluxDBSink.DB_USERNAME_KEY, "test");
    defaultConf.put(InfluxDBSink.DB_PASSWORD_KEY, "password123");
    defaultConf.put(InfluxDBSink.BATCH_PROCESS_KEY, "false");

    influx.init(defaultConf, context);
  }

  @Test
  public void testCreateWithBatch() {
    InfluxDBSink influx = new InfluxDBSink();

    defaultConf.put(InfluxDBSink.BATCH_PROCESS_KEY, "true");
    defaultConf.put(InfluxDBSink.MAIN_BUFFER_KEY, 1000);
    defaultConf.put(InfluxDBSink.FAIL_BUFFER_KEY, 1000);
    defaultConf.put("flush-frequency-ms", 60000);

    influx.init(defaultConf, context);

  }

  @Test
  public void testCreateWithoutBatch() {
    InfluxDBSink influx = new InfluxDBSink();

    defaultConf.put(InfluxDBSink.BATCH_PROCESS_KEY, "false");

    influx.init(defaultConf, context);
  }

  @Test
  public void testMetricSend() {
    InfluxDBSink influx = new InfluxDBSink();

    defaultConf.put(InfluxDBSink.BATCH_PROCESS_KEY, "false");

    influx.init(defaultConf, context);

    for(MetricsRecord record : records){
      influx.processRecord(record);
    }

  }
}
