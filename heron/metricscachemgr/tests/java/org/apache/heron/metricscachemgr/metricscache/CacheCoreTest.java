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

package org.apache.heron.metricscachemgr.metricscache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.heron.metricscachemgr.metricscache.query.MetricDatum;
import org.apache.heron.metricscachemgr.metricscache.query.MetricRequest;
import org.apache.heron.metricscachemgr.metricscache.query.MetricResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricTimeRangeValue;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.metrics.MetricsFilter;

import static org.apache.heron.metricscachemgr.metricscache.query.MetricGranularity.RAW;
import static org.junit.Assert.assertEquals;

public class CacheCoreTest {
  // test target
  private static CacheCore cacheCore;
  // cache timestamp
  private static long now;
  // aggregation type
  private static MetricsFilter metricsFilter;

  // sort MetricTimeRangeValue
  private static Comparator<MetricTimeRangeValue> timeRangeValueComparator;

  // sort MetricDatum
  private static Comparator<MetricDatum> datumComparator;

  private static void assertMetricValue(
      List<MetricTimeRangeValue> expected, List<MetricTimeRangeValue> actualIn) {
    List<MetricTimeRangeValue> actual = new ArrayList<>(actualIn);
    actual.sort(timeRangeValueComparator);
    int len = expected.size();
    assertEquals(len, actual.size());
    for (int i = 0; i < len; i++) {
      MetricTimeRangeValue expectedVal = expected.get(i);
      MetricTimeRangeValue actualVal = actual.get(i);
      assertEquals(expectedVal.getStartTime(), actualVal.getStartTime());
      assertEquals(expectedVal.getEndTime(), actualVal.getEndTime());
      assertEquals(expectedVal.getValue(), actualVal.getValue());
    }
  }

  private static void assertMetricResponse(
      List<MetricDatum> metricListIn, MetricDatum... metricData) {
    List<MetricDatum> metricList = new ArrayList<>(metricListIn);
    assertEquals(metricData.length, metricList.size());
    metricList.sort(datumComparator);
    for (int i = 0; i < metricData.length; i++) {
      MetricDatum expected = metricData[i];
      MetricDatum actual = metricList.get(i);
      assertEquals(expected.getComponentName(), actual.getComponentName());
      assertEquals(expected.getInstanceId(), actual.getInstanceId());
      assertEquals(expected.getMetricName(), actual.getMetricName());
      assertMetricValue(expected.getMetricValue(), actual.getMetricValue());
    }
  }

  private void prepareDataForHashIndex() {
    // create cache with time window 100 seconds, bucket size 30 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-100 seconds ~ now-70 seconds)
    // bucket 2: [now-70 seconds ~ now-40 seconds)
    // bucket 3: [now-40 seconds ~ now-10 seconds)
    // bucket 4: [now-10 seconds ~ now]
    cacheCore = new CacheCore(Duration.ofSeconds(100), Duration.ofSeconds(30), 0);

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    now = System.currentTimeMillis();
    TopologyManager.PublishMetrics.Builder builder = TopologyManager.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 90 * 1000;

    String[] components = new String[]{
        "c1", "c2"
    };
    String[] instances = new String[]{
        "i1", "i2"
    };
    String[] metrics = new String[]{
        "m1", "m2"
    };
    String[] vals = new String[]{
        "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8"
    };
    int valIdx = 0;
    for (String component : components) {
      for (String instance : instances) {
        for (String metric : metrics) {
          builder.addMetrics(TopologyManager.MetricDatum.newBuilder()
              .setTimestamp(ts)
              .setComponentName(component).setInstanceId(instance)
              .setName(metric)
              .setValue(vals[valIdx++]));
        }
      }
    }

    cacheCore.addMetricException(builder.build());

    metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);
    metricsFilter.setMetricToType("m2", MetricsFilter.MetricAggregationType.SUM);

    datumComparator =
        new Comparator<MetricDatum>() {
          @Override
          public int compare(MetricDatum o1,
                             MetricDatum o2) {
            if (!o1.getComponentName().equals(o2.getComponentName())) {
              return o1.getComponentName().compareTo(o2.getComponentName());
            }
            if (!o1.getInstanceId().equals(o2.getInstanceId())) {
              return o1.getInstanceId().compareTo(o2.getInstanceId());
            }
            if (!o1.getMetricName().equals(o2.getMetricName())) {
              return o1.getMetricName().compareTo(o2.getMetricName());
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
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    assertMetricResponse(response.getMetricList(),
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        ))
    );
  }

  /*
   * query instances: null
   */
  @Test
  public void testInstanceNull() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", null);
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.3")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }


  /*
   * query instances: i1, i2
   */
  @Test
  public void testInstances() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    componentNameInstanceId.get("c1").add("i2");
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.3")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query instances: empty
   */
  @Test
  public void testInstanceEmpty() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    assertEquals(response.getMetricList().size(), 0);
  }

  /*
   * query components: null
   */
  @Test
  public void testComponentNull() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(null, metricNames, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.3")
        )),
        new MetricDatum("c2", "i1", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.5")
        )),
        new MetricDatum("c2", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.7")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query components: c1, c2
   */
  @Test
  public void testComponent() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", null);
    componentNameInstanceId.put("c2", null);
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.3")
        )),
        new MetricDatum("c2", "i1", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.5")
        )),
        new MetricDatum("c2", "i2", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.7")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query components: empty
   */
  @Test
  public void testComponents() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    assertEquals(response.getMetricList().size(), 0);
  }

  /*
   * query metrics: m1, m2
   */
  @Test
  public void testMetricsSameComponentInstance() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    metricNames.add("m2");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i1", "m2", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.2")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query metrics: c1, c2, i1, m1, m2
   */
  @Test
  public void testMetrics() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    componentNameInstanceId.put("c2", new HashSet<String>());
    componentNameInstanceId.get("c2").add("i1");
    Set<String> metricNames = new HashSet<String>();
    metricNames.add("m1");
    metricNames.add("m2");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 4 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i1", "m2", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.2")
        )),
        new MetricDatum("c2", "i1", "m1", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.5")
        )),
        new MetricDatum("c2", "i1", "m2", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.6")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query metrics: null
   */
  @Test
  public void testMetricsNull() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    MetricRequest request =
        new MetricRequest(componentNameInstanceId, null, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 2 <component, instance, metric> tuples
    MetricDatum[] expected = new MetricDatum[]{
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1")
        )),
        new MetricDatum("c1", "i1", "m2", Arrays.asList(
            new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.2")
        ))
    };
    assertMetricResponse(response.getMetricList(), expected);
  }

  /*
   * query metrics: empty
   */
  @Test
  public void testMetricsEmpty() {
    prepareDataForHashIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 85 * 1000;
    Map<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    Set<String> metricNames = new HashSet<String>();
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is 0 <component, instance, metric> tuples
    assertEquals(response.getMetricList().size(), 0);
  }

  private void prepareDataForTreeIndex() {
    // create cache with time window 100 seconds, bucket size 30 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-100 seconds ~ now-70 seconds)
    // bucket 2: [now-70 seconds ~ now-40 seconds)
    // bucket 3: [now-40 seconds ~ now-10 seconds)
    // bucket 4: [now-10 seconds ~ now]
    cacheCore = new CacheCore(Duration.ofSeconds(100), Duration.ofSeconds(30), 0);

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    now = System.currentTimeMillis();
    TopologyManager.PublishMetrics.Builder builder = TopologyManager.PublishMetrics.newBuilder();
    long[] ts = new long[]{
        // the timestamp falls outside cache time window. too old to be in the cache
        now - 120 * 1000,
        // should be in bucket 1
        now - 90 * 1000,
        // should be in bucket 1
        now - 80 * 1000,
        // should be in bucket 2
        now - 60 * 1000,
        // should be in bucket 2
        now - 50 * 1000,
        // should be in bucket 3
        now - 30 * 1000,
        // should be in bucket 3
        now - 20 * 1000,
        // should be in bucket 4
        now
    };
    String[] vals = new String[]{
        "0.0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7"
    };
    for (int i = 0; i < ts.length; i++) {
      builder.addMetrics(TopologyManager.MetricDatum.newBuilder()
          .setTimestamp(ts[i])
          .setComponentName("c1").setInstanceId("i1")
          .setName("m1")
          .setValue(vals[i]));
    }

    cacheCore.addMetricException(builder.build());

    // initialization
    metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    timeRangeValueComparator =
        new Comparator<MetricTimeRangeValue>() {
          @Override
          public int compare(MetricTimeRangeValue o1,
                             MetricTimeRangeValue o2) {
            return (int) (o1.getStartTime() - o2.getStartTime());
          }
        };
  }

  /*
   * query 1 bucket
   */
  @Test
  public void testTreeIndex1() {
    prepareDataForTreeIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 75 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    List<MetricDatum> metricList = response.getMetricList();
    assertEquals(metricList.size(), 1);
    // there should be 2 metrics
    List<MetricTimeRangeValue> expected = Arrays.asList(
        new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1"),
        new MetricTimeRangeValue(now - 80 * 1000, now - 80 * 1000, "0.2")
    );
    assertMetricValue(expected, metricList.get(0).getMetricValue());
  }

  /*
   * query 2 buckets
   */
  @Test
  public void testTreeIndex2() {
    prepareDataForTreeIndex();

    long startTime = now - 95 * 1000;
    long endTime = now - 45 * 1000;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    List<MetricDatum> metricList = response.getMetricList();
    assertEquals(metricList.size(), 1);
    // there should be 4 metrics
    List<MetricTimeRangeValue> expected = Arrays.asList(
        new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1"),
        new MetricTimeRangeValue(now - 80 * 1000, now - 80 * 1000, "0.2"),
        new MetricTimeRangeValue(now - 60 * 1000, now - 60 * 1000, "0.3"),
        new MetricTimeRangeValue(now - 50 * 1000, now - 50 * 1000, "0.4")
    );
    assertMetricValue(expected, metricList.get(0).getMetricValue());
  }

  /*
   * query all buckets
   */
  @Test
  public void testTreeIndexAll() {
    prepareDataForTreeIndex();

    long startTime = now - 200 * 1000;
    long endTime = now;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    List<MetricDatum> metricList = response.getMetricList();
    assertEquals(metricList.size(), 1);
    // there should be 7 metrics
    List<MetricTimeRangeValue> expected = Arrays.asList(
        new MetricTimeRangeValue(now - 90 * 1000, now - 90 * 1000, "0.1"),
        new MetricTimeRangeValue(now - 80 * 1000, now - 80 * 1000, "0.2"),
        new MetricTimeRangeValue(now - 60 * 1000, now - 60 * 1000, "0.3"),
        new MetricTimeRangeValue(now - 50 * 1000, now - 50 * 1000, "0.4"),
        new MetricTimeRangeValue(now - 30 * 1000, now - 30 * 1000, "0.5"),
        new MetricTimeRangeValue(now - 20 * 1000, now - 20 * 1000, "0.6"),
        new MetricTimeRangeValue(now, now, "0.7")
    );
    assertMetricValue(expected, metricList.get(0).getMetricValue());
  }

  /*
   * query the last bucket
   */
  @Test
  public void testTreeIndexLast() {
    prepareDataForTreeIndex();

    long startTime = now - 5 * 1000;
    long endTime = now;
    MetricRequest request = new MetricRequest(null, null, startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    List<MetricDatum> metricList = response.getMetricList();
    assertEquals(metricList.size(), 1);
    // there should be 1 metric
    List<MetricTimeRangeValue> list = new ArrayList<>(metricList.get(0).getMetricValue());
    assertEquals(list.size(), 1);
    // check value
    assertEquals(list.get(0).getValue(), "0.7");
  }

  @Test
  public void testPurge() throws InterruptedException {
    // create cache with time window 10 seconds, bucket size 3 seconds and no exception store.
    // the cache should be initialized with 4 buckets:
    // bucket 1: [now-10 seconds ~ now-7 seconds)
    // bucket 2: [now-7 seconds ~ now-4 seconds)
    // bucket 3: [now-4 seconds ~ now-1 seconds)
    // bucket 4: [now-1 seconds ~ now]
    FakeTicker ticker = new FakeTicker();
    cacheCore = new CacheCore(Duration.ofSeconds(10), Duration.ofSeconds(3), 0, ticker);

    // current timestamp used as time origin
    // although it may be slightly different from the time origin
    // in the CacheCore initialization.
    now = ticker.read();

    TopologyManager.PublishMetrics.Builder builder = TopologyManager.PublishMetrics.newBuilder();
    // should be in bucket 1
    long ts = now - 9 * 1000;
    // c1-i1, m1: 0.1
    builder.addMetrics(TopologyManager.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.1"));

    cacheCore.addMetricException(builder.build());

    metricsFilter = new MetricsFilter();
    metricsFilter.setMetricToType("m1", MetricsFilter.MetricAggregationType.SUM);

    // query before purge
    long startTime = now - 20 * 1000;
    long endTime = now;
    HashMap<String, Set<String>> componentNameInstanceId = new HashMap<>();
    componentNameInstanceId.put("c1", new HashSet<String>());
    componentNameInstanceId.get("c1").add("i1");
    Set<String> metricNames = new HashSet<>();
    metricNames.add("m1");
    MetricRequest request = new MetricRequest(componentNameInstanceId, metricNames,
        startTime, endTime, RAW);
    MetricResponse response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    assertMetricResponse(response.getMetricList(),
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 9 * 1000, now - 9 * 1000, "0.1")
        ))
    );

    // purge
    ticker.advance(Duration.ofSeconds(3)); // assure more than 1 bucket is purged
    cacheCore.purge();

    // query after purge
    response = cacheCore.getMetrics(request, metricsFilter);
    // there is 1 <component, instance, metric> tuple: how to trim the gone metric in metadata?
    assertMetricResponse(response.getMetricList(),
        new MetricDatum("c1", "i1", "m1", Arrays.asList(new MetricTimeRangeValue[]{}))
    );

    // insert-select after purge
    TopologyManager.PublishMetrics.Builder builder2 = TopologyManager.PublishMetrics.newBuilder();
    // should be in bucket 1
    ts = now - 3 * 1000;
    // c1-i1, m1: 0.1
    builder2.addMetrics(TopologyManager.MetricDatum.newBuilder()
        .setTimestamp(ts)
        .setComponentName("c1").setInstanceId("i1")
        .setName("m1")
        .setValue("0.2"));

    cacheCore.addMetricException(builder2.build());

    response = cacheCore.getMetrics(request, metricsFilter);
    // there is only one <component, instance, metric> tuple
    assertMetricResponse(response.getMetricList(),
        new MetricDatum("c1", "i1", "m1", Arrays.asList(
            // there should be 1 metric for each instance
            new MetricTimeRangeValue(now - 3 * 1000, now - 3 * 1000, "0.2")
        ))
    );
  }

  private static final class FakeTicker extends CacheCore.Ticker {
    private AtomicLong now = new AtomicLong(System.currentTimeMillis());

    void advance(Duration duration) {
      now.addAndGet(duration.toMillis());
    }

    @Override
    long read() {
      return now.get();
    }
  }
}
