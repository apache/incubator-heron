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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

import static com.twitter.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.Granularity.RAW;

public class CacheCoreTestHashIndex {
  // test target
  private static CacheCore cacheCore;
  // cache timestamp
  private static long now;
  // aggregation type
  private static MetricsFilter metricsFilter;
  // sort
  private static Comparator<MetricsCacheQueryUtils.MetricDatum> comparator;

  @BeforeClass
  public static void insertData() {
    // create cache with time window 100 seconds, bucket size 30 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-100 seconds ~ now-70 seconds)
    // bucket 2: [now-70 seconds ~ now-40 seconds)
    // bucket 3: [now-40 seconds ~ now-10 seconds)
    // bucket 4: [now-10 seconds ~ now]
    cacheCore = new CacheCore(100, 30, 0);

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    now = System.currentTimeMillis();
    TopologyMaster.PublishMetrics.Builder builder = TopologyMaster.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 90 * 1000;
    // c1-i1, m1: 0.1
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));
    // c1-i1, m2: 0.2
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m2")
        .setValue("0.2"));
    // c1-i2, m1: 0.3
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i2")
        .setName("m1")
        .setValue("0.3"));
    // c1-i2, m2: 0.4
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i2")
        .setName("m2")
        .setValue("0.4"));
    // c2-i1, m1: 0.5
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i1")
        .setName("m1")
        .setValue("0.5"));
    // c2-i1, m2: 0.6
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i1")
        .setName("m2")
        .setValue("0.6"));
    // c2-i2, m1: 0.7
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i2")
        .setName("m1")
        .setValue("0.7"));
    // c2-i2, m2: 0.8
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c2").setInstanceId("i2")
        .setName("m2")
        .setValue("0.8"));

    cacheCore.addMetricException(builder.build());

    metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);
    metricsFilter.setMetricToType("m2", MetricsFilter.MetricAggregationType.SUM);

    comparator =
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

  }

  /*
   * query 1 metric
   */
  @Test
  public void test1() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    // check value
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
  }

  /*
   * query instances: null
   */
  @Test
  public void testInstanceNull() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", null);
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.3");
  }


  /*
   * query instances: i1, i2
   */
  @Test
  public void testInstances() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.componentNameInstanceId.get("c1").add("i2");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.3");
  }

  /*
   * query instances: empty
   */
  @Test
  public void testInstanceEmpty() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);
  }

  /*
   * query components: null
   */
  @Test
  public void testComponentNull() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(2).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(3).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.3");
    Assert.assertEquals(response.metricList.get(2).getMetricValue().get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).getMetricValue().get(0).value, "0.7");
  }

  /*
   * query components: c1, c2
   */
  @Test
  public void testComponent() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", null);
    request.componentNameInstanceId.put("c2", null);
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(2).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(3).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.3");
    Assert.assertEquals(response.metricList.get(2).getMetricValue().get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).getMetricValue().get(0).value, "0.7");
  }

  /*
   * query components: empty
   */
  @Test
  public void testComponents() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);
  }

  /*
   * query metrics: m1, m2
   */
  @Test
  public void testMetricsSameComponentInstance() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    request.metricNames.add("m2");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.2");
  }

  /*
   * query metrics: c1, c2, i1, m1, m2
   */
  @Test
  public void testMetrics() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.componentNameInstanceId.put("c2", new HashSet<String>());
    request.componentNameInstanceId.get("c2").add("i1");
    request.metricNames = new HashSet<String>();
    request.metricNames.add("m1");
    request.metricNames.add("m2");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(2).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(3).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.2");
    Assert.assertEquals(response.metricList.get(2).getMetricValue().get(0).value, "0.5");
    Assert.assertEquals(response.metricList.get(3).getMetricValue().get(0).value, "0.6");
  }

  /*
   * query metrics: null
   */
  @Test
  public void testMetricsNull() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
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
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    Assert.assertEquals(response.metricList.get(1).getMetricValue().size(), 1);
    // check values
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.1");
    Assert.assertEquals(response.metricList.get(1).getMetricValue().get(0).value, "0.2");
  }

  /*
   * query metrics: empty
   */
  @Test
  public void testMetricsEmpty() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 85 * 1000;
    request.aggregationGranularity = RAW;
    request.componentNameInstanceId = new HashMap<>();
    request.componentNameInstanceId.put("c1", new HashSet<String>());
    request.componentNameInstanceId.get("c1").add("i1");
    request.metricNames = new HashSet<String>();
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    Assert.assertEquals(response.metricList.size(), 0);
  }
}
