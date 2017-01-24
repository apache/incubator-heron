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


package com.twitter.heron.metricscachemgr.metricscache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

public class CacheCoreTest {
  private static String debugFilePath =
      "/tmp/" + CacheCoreTest.class.getSimpleName() + ".debug.txt";
  private static Path file = null;
  private static List<String> lines = null;

  @BeforeClass
  public static void before() throws Exception {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();

    lines.add(Paths.get(".").toAbsolutePath().normalize().toString());
  }

  @AfterClass
  public static void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  @Test
  public void testMetricTreeIndex() {
    lines.add("testMetricTreeIndex");
    // create cache with time window 100 seconds, bucket size 30 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-100 seconds ~ now-70 seconds)
    // bucket 2: [now-70 seconds ~ now-40 seconds)
    // bucket 3: [now-40 seconds ~ now-10 seconds)
    // bucket 4: [now-10 seconds ~ now]
    CacheCore cacheCore = new CacheCore(100, 30, 0);
    lines.add(cacheCore.toString());

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    long now = System.currentTimeMillis();
    lines.add(String.valueOf(now));
    TopologyMaster.PublishMetrics.Builder builder = TopologyMaster.PublishMetrics.newBuilder();
    long ts = 0;
    // the timestamp falls outside cache time window. too old to be in the cache
    ts = now - 120 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.0"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 1
    ts = now - 90 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 1
    ts = now - 80 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.2"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 2
    ts = now - 60 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.3"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 2
    ts = now - 50 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.4"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 3
    ts = now - 30 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.5"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 3
    ts = now - 20 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.6"));
    lines.add("insert " + String.valueOf(ts));
    // should be in bucket 4
    ts = now;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.7"));
    lines.add("insert " + String.valueOf(ts));

    cacheCore.addMetricException(builder.build());
    lines.add("addMetricException: " + cacheCore.toString());

    MetricsFilter metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    Comparator<MetricsCacheQueryUtils.MetricTimeRangeValue> comparator =
        new Comparator<MetricsCacheQueryUtils.MetricTimeRangeValue>() {
          @Override
          public int compare(MetricsCacheQueryUtils.MetricTimeRangeValue o1,
                             MetricsCacheQueryUtils.MetricTimeRangeValue o2) {
            return (int) (o1.startTime - o2.startTime);
          }
        };

    /*
     * query 1 bucket
      */
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 75 * 1000;
    request.aggregationGranularity = 2;
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 2 metrics
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 2);
    // sort
    response.metricList.get(0).metricValue.sort(comparator);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(1).value, "0.2");

    /*
     * query 2 buckets
      */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 45 * 1000;
    request.aggregationGranularity = 2;
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 4 metrics
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 4);
    // sort
    response.metricList.get(0).metricValue.sort(comparator);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(1).value, "0.2");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(2).value, "0.3");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(3).value, "0.4");

    /*
     * query all buckets
      */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 200 * 1000;
    request.endTime = now;
    request.aggregationGranularity = 2;
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 7 metrics
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 7);
    // sort
    response.metricList.get(0).metricValue.sort(comparator);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(1).value, "0.2");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(2).value, "0.3");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(3).value, "0.4");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(4).value, "0.5");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(5).value, "0.6");
    Assert.assertEquals(response.metricList.get(0).metricValue.get(6).value, "0.7");

    /*
     * query the last bucket
      */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 5 * 1000;
    request.endTime = now;
    request.aggregationGranularity = 2;
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    // sort
    response.metricList.get(0).metricValue.sort(comparator);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.7");
  }

  @Test
  public void testMetricHashIndex() {
    lines.add("testMetricHashIndex");
    // create cache with time window 100 seconds, bucket size 30 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-100 seconds ~ now-70 seconds)
    // bucket 2: [now-70 seconds ~ now-40 seconds)
    // bucket 3: [now-40 seconds ~ now-10 seconds)
    // bucket 4: [now-10 seconds ~ now]
    CacheCore cacheCore = new CacheCore(100, 30, 0);
    lines.add(cacheCore.toString());

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    long now = System.currentTimeMillis();
    lines.add(String.valueOf(now));
    TopologyMaster.PublishMetrics.Builder builder = TopologyMaster.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 90 * 1000;
    // c1-i1, m1: 0.1
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));
    lines.add("insert c1-i1, m1: 0.1");
    // c1-i1, m2: 0.2
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m2")
        .setValue("0.2"));
    lines.add("insert c1-i1, m2: 0.2");
    // c1-i2, m1: 0.3
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i2")
        .setName("m1")
        .setValue("0.3"));
    lines.add("insert c1-i2, m1: 0.3");
    // c1-i2, m2: 0.4
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i2")
        .setName("m2")
        .setValue("0.4"));
    lines.add("insert c1-i2, m2: 0.4");
    // c2-i1, m1: 0.5
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i1")
        .setName("m1")
        .setValue("0.5"));
    lines.add("insert c2-i1, m1: 0.5");
    // c2-i1, m2: 0.6
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i1")
        .setName("m2")
        .setValue("0.6"));
    lines.add("insert c2-i1, m2: 0.6");
    // c2-i2, m1: 0.7
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i2")
        .setName("m1")
        .setValue("0.7"));
    lines.add("insert c2-i2, m1: 0.7");
    // c2-i2, m2: 0.8
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i2")
        .setName("m2")
        .setValue("0.8"));
    lines.add("insert c2-i2, m2: 0.8");

    cacheCore.addMetricException(builder.build());
    lines.add("addMetricException: " + cacheCore.toString());

    MetricsFilter metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);
    metricsFilter.setMetricToType("m2", MetricsFilter.MetricAggregationType.SUM);

    Comparator<MetricsCacheQueryUtils.MetricDatum> comparator =
        new Comparator<MetricsCacheQueryUtils.MetricDatum>() {
          @Override
          public int compare(MetricsCacheQueryUtils.MetricDatum o1,
                             MetricsCacheQueryUtils.MetricDatum o2) {
            if (!o1.componentName.equals(o2.componentName)) {
              return o1.componentName.compareTo(o2.componentName);
            }
            if (!o1.instanceId.equals(o2.instanceId)) {
              return o1.instanceId.compareTo(o2.instanceId);
            }
            if (!o1.metricName.equals(o2.metricName)) {
              return o1.metricName.compareTo(o2.metricName);
            }
            return 0;
          }
        };

    /*
     * query 1 metric
     */
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");

    /*
     * query instances: null
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", null);
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 2);
    // sort
    response.metricList.sort(comparator);
    // there should be 2 instances
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(1).metricName, "m1");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.3");

    /*
     * query instances: i1, i2
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.componentNameInstanceId.get("c1").add("i2");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 2);
    // sort
    response.metricList.sort(comparator);
    // there should be 2 instances
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(1).metricName, "m1");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.3");

    /*
     * query instances: empty
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);

    /*
     * query components: null
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 4);
    // sort
    response.metricList.sort(comparator);
    // there should be 4 component-instance
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(1).metricName, "m1");
    Assert.assertEquals(response.metricList.get(2).componentName, "c2");
    Assert.assertEquals(response.metricList.get(2).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(2).metricName, "m1");
    Assert.assertEquals(response.metricList.get(3).componentName, "c2");
    Assert.assertEquals(response.metricList.get(3).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(3).metricName, "m1");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(2).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(3).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.3");
    Assert.assertEquals(response.metricList.get(2).metricValue.get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).metricValue.get(0).value, "0.7");

    /*
     * query components: c1, c2
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", null);
    request.componentNameInstanceId.put("c2", null);
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 4);
    // sort
    response.metricList.sort(comparator);
    // there should be 4 component-instance
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(1).metricName, "m1");
    Assert.assertEquals(response.metricList.get(2).componentName, "c2");
    Assert.assertEquals(response.metricList.get(2).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(2).metricName, "m1");
    Assert.assertEquals(response.metricList.get(3).componentName, "c2");
    Assert.assertEquals(response.metricList.get(3).instanceId, "i2");
    Assert.assertEquals(response.metricList.get(3).metricName, "m1");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(2).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(3).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.3");
    Assert.assertEquals(response.metricList.get(2).metricValue.get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).metricValue.get(0).value, "0.7");

    /*
     * query components: empty
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);

    /*
     * query metrics: m1, m2
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    request.metricNames.add("m2");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 2);
    // sort
    response.metricList.sort(comparator);
    // there should be 2 metrics
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(1).metricName, "m2");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.2");

    /*
     * query metrics: c1, c2, i1, m1, m2
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.componentNameInstanceId.put("c2", new HashSet<String>());
    request.componentNameInstanceId.get("c2").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    request.metricNames.add("m2");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 4);
    // sort
    response.metricList.sort(comparator);
    // there should be 4 metrics
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(1).metricName, "m2");
    Assert.assertEquals(response.metricList.get(2).componentName, "c2");
    Assert.assertEquals(response.metricList.get(2).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(2).metricName, "m1");
    Assert.assertEquals(response.metricList.get(3).componentName, "c2");
    Assert.assertEquals(response.metricList.get(3).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(3).metricName, "m2");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(2).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(3).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.2");
    Assert.assertEquals(response.metricList.get(2).metricValue.get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).metricValue.get(0).value, "0.6");

    /*
     * query metrics: null
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 2);
    // sort
    response.metricList.sort(comparator);
    // there should be 2 metrics
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    Assert.assertEquals(response.metricList.get(1).componentName, "c1");
    Assert.assertEquals(response.metricList.get(1).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(1).metricName, "m2");
    // there should be 1 metric for each instance
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    Assert.assertEquals(response.metricList.get(1).metricValue.size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).metricValue.get(0).value, "0.2");

    /*
     * query metrics: empty
     */
    request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);
  }

  @Test
  public void testPurge() throws InterruptedException {
    lines.add("testPurge");
    // create cache with time window 10 seconds, bucket size 3 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-10 seconds ~ now-7 seconds)
    // bucket 2: [now-7 seconds ~ now-4 seconds)
    // bucket 3: [now-4 seconds ~ now-1 seconds)
    // bucket 4: [now-1 seconds ~ now]
    CacheCore cacheCore = new CacheCore(10, 3, 0);
    lines.add(cacheCore.toString());

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    long now = System.currentTimeMillis();
    lines.add(String.valueOf(now));

    TopologyMaster.PublishMetrics.Builder builder = TopologyMaster.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 9 * 1000;
    // c1-i1, m1: 0.1
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));
    lines.add("insert c1-i1, m1: 0.1");

    cacheCore.addMetricException(builder.build());
    lines.add("addMetricException: " + cacheCore.toString());

    MetricsFilter metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    // query before purge
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 20 * 1000;
    request.endTime = now;
    request.aggregationGranularity = 2;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.1");

    // purge
    Thread.sleep(3 * 1000); // assure more than 1 bucket is purged
    cacheCore.purge();
    lines.add("after purge: " + cacheCore.toString());

    // query after purge
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 1 <component, instance, metric> tuple: how to trim the gone metric in metadata?
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there is no metric value
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 0);

    // insert-select after purge
    TopologyMaster.PublishMetrics.Builder builder2 = TopologyMaster.PublishMetrics.newBuilder();
    // should be in bucket 1
    ts = now - 3 * 1000;
    // c1-i1, m1: 0.1
    builder2.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.2"));
    lines.add("insert c1-i1, m1: 0.2");

    cacheCore.addMetricException(builder2.build());
    lines.add("addMetricException: " + cacheCore.toString());

    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).metricValue.size(), 1);
    // check value
    Assert.assertEquals(response.metricList.get(0).metricValue.get(0).value, "0.2");
  }

}
