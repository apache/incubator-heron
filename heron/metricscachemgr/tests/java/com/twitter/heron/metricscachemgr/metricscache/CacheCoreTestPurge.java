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

import java.util.HashMap;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

import static com.twitter.heron.metricscachemgr.metricscache.MetricsCacheQueryUtils.Granularity.RAW;

public class CacheCoreTestPurge {
//  private static String debugFilePath =
//      "/tmp/" + CacheCoreTest.class.getSimpleName() + ".debug.txt";
//  private static Path file = null;
//  private static List<String> lines = null;

//  @BeforeClass
//  public static void before() throws Exception {
//    file = Paths.get(debugFilePath);
//    lines = new ArrayList<>();
//
//    lines.add(Paths.get(".").toAbsolutePath().normalize().toString());
//  }
//
//  @AfterClass
//  public static void after() throws IOException {
//    Files.write(file, lines, Charset.forName("UTF-8"));
//  }


  @Test
  public void testPurge() throws InterruptedException {
    // create cache with time window 10 seconds, bucket size 3 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-10 seconds ~ now-7 seconds)
    // bucket 2: [now-7 seconds ~ now-4 seconds)
    // bucket 3: [now-4 seconds ~ now-1 seconds)
    // bucket 4: [now-1 seconds ~ now]
    CacheCore cacheCore = new CacheCore(10, 3, 0);

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    long now = System.currentTimeMillis();

    TopologyMaster.PublishMetrics.Builder builder = TopologyMaster.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 9 * 1000;
    // c1-i1, m1: 0.1
    builder.addMetrics(TopologyMaster.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));

    cacheCore.addMetricException(builder.build());

    MetricsFilter metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    // query before purge
    MetricsCacheQueryUtils.MetricRequest request = new MetricsCacheQueryUtils.MetricRequest();
    request.startTime = now - 20 * 1000;
    request.endTime = now;
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

    // purge
    Thread.sleep(3 * 1000); // assure more than 1 bucket is purged
    cacheCore.purge();

    // query after purge
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 1 <component, instance, metric> tuple: how to trim the gone metric in metadata?
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there is no metric value
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 0);

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

    cacheCore.addMetricException(builder2.build());

    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    Assert.assertEquals(response.metricList.size(), 1);
    Assert.assertEquals(response.metricList.get(0).componentName, "c1");
    Assert.assertEquals(response.metricList.get(0).instanceId, "i1");
    Assert.assertEquals(response.metricList.get(0).metricName, "m1");
    // there should be 1 metric
    Assert.assertEquals(response.metricList.get(0).getMetricValue().size(), 1);
    // check value
    Assert.assertEquals(response.metricList.get(0).getMetricValue().get(0).value, "0.2");
  }

}
