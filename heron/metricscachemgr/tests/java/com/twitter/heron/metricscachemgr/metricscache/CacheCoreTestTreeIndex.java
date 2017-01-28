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
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

import static com.twitter.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.Granularity.RAW;

public class CacheCoreTestTreeIndex {
  // test target
  private static CacheCore cacheCore;
  // cache timestamp
  private static long now;
  // aggregation type
  private static MetricsFilter metricsFilter;
  // sort MetricTimeRangeValue
  private static Comparator<MetricsCacheQueryUtils.MetricTimeRangeValue> comparator;

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
    long ts = 0;
    // the timestamp falls outside cache time window. too old to be in the cache
    ts = now - 120 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.0"));
    // should be in bucket 1
    ts = now - 90 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));
    // should be in bucket 1
    ts = now - 80 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.2"));
    // should be in bucket 2
    ts = now - 60 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.3"));
    // should be in bucket 2
    ts = now - 50 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.4"));
    // should be in bucket 3
    ts = now - 30 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.5"));
    // should be in bucket 3
    ts = now - 20 * 1000;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.6"));
    // should be in bucket 4
    ts = now;
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.7"));

    cacheCore.addMetricException(builder.build());

    // initialization
    metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    comparator =
        new Comparator<MetricsCacheQueryUtils.MetricTimeRangeValue>() {
          @Override
          public int compare(MetricsCacheQueryUtils.MetricTimeRangeValue o1,
                             MetricsCacheQueryUtils.MetricTimeRangeValue o2) {
            return (int) (o1.startTime - o2.startTime);
          }
        };
  }

  /*
   * query 1 bucket
   */
  @Test
  public void test1() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 75 * 1000;
    request.aggregationGranularity = RAW;
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 2 metrics
    List<MetricsCacheQueryUtils.MetricTimeRangeValue> list =
        response.metricList.get(0).getMetricValue();
    Assert.assertEquals(list.size(), 2);
    // sort
    list.sort(comparator);
    // check values
    Assert.assertEquals(list.get(0).value, "0.1");
    Assert.assertEquals(list.get(1).value, "0.2");
  }

  /*
   * query 2 buckets
   */
  @Test
  public void test2() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 95 * 1000;
    request.endTime = now - 45 * 1000;
    request.aggregationGranularity = RAW;
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 4 metrics
    List<MetricsCacheQueryUtils.MetricTimeRangeValue> list =
        response.metricList.get(0).getMetricValue();
    Assert.assertEquals(list.size(), 4);
    // sort
    list.sort(comparator);
    // check value
    Assert.assertEquals(list.get(0).value, "0.1");
    Assert.assertEquals(list.get(1).value, "0.2");
    Assert.assertEquals(list.get(2).value, "0.3");
    Assert.assertEquals(list.get(3).value, "0.4");
  }

  /*
   * query all buckets
   */
  @Test
  public void testAll() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 200 * 1000;
    request.endTime = now;
    request.aggregationGranularity = RAW;
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 7 metrics
    List<MetricsCacheQueryUtils.MetricTimeRangeValue> list =
        response.metricList.get(0).getMetricValue();
    Assert.assertEquals(list.size(), 7);
    // sort
    list.sort(comparator);
    // check value
    Assert.assertEquals(list.get(0).value, "0.1");
    Assert.assertEquals(list.get(1).value, "0.2");
    Assert.assertEquals(list.get(2).value, "0.3");
    Assert.assertEquals(list.get(3).value, "0.4");
    Assert.assertEquals(list.get(4).value, "0.5");
    Assert.assertEquals(list.get(5).value, "0.6");
    Assert.assertEquals(list.get(6).value, "0.7");
  }

  /*
   * query the last bucket
   */
  @Test
  public void testLast() {
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 5 * 1000;
    request.endTime = now;
    request.aggregationGranularity = RAW;
    MetricsCacheQueryUtils.MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    // sort
    response.metricList.get(0).getMetricValue().sort(comparator);
    // check value
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.7");
  }
}
