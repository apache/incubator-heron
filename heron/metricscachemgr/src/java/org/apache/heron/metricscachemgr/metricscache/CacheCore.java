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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;

import org.apache.heron.common.basics.WakeableLooper;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionDatum;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionRequest;
import org.apache.heron.metricscachemgr.metricscache.query.ExceptionResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricDatum;
import org.apache.heron.metricscachemgr.metricscache.query.MetricGranularity;
import org.apache.heron.metricscachemgr.metricscache.query.MetricRequest;
import org.apache.heron.metricscachemgr.metricscache.query.MetricResponse;
import org.apache.heron.metricscachemgr.metricscache.query.MetricTimeRangeValue;
import org.apache.heron.metricscachemgr.metricscache.store.ExceptionDatapoint;
import org.apache.heron.metricscachemgr.metricscache.store.MetricDatapoint;
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * Cache Engine to store metrics and exceptions in memory and to respond to query,
 * implementing insertion and selection methods:
 * 1. Storage for metrics: timestamp_start -(tree)-&gt; [sparse: bucket_id -(map)-&gt; metric bucket]
 * 2. Storage for exceptions: idxComponentInstance -(hash)-&gt; exception bucket
 * 3. Index for metrics:
 * a. metricName -(map)-&gt; idxMetricName (int: locator)
 * b. component -(map)-&gt; instance -(map)-&gt; idxComponentInstance (int: locator)
 * bucket_id is Long from idxComponentInstance Integer and idxMetricName Integer combined
 * 4. Index for exceptions:
 * component -(map)-&gt; instance -(map)-&gt; idxComponentInstance (int: locator)
 * 5. Query pattern: component-instance (equality), metricName (equality), timestamp (range)
 * Different from tmanager:
 * 1. order bucket by metric timestamp rather than metric message arriving time
 * 2. free buckets for instances that are gone during scaling process
 * 3. lock for multiple threads
 * Same as tmanager:
 * 1. support same protobuf message/request format
 */
public class CacheCore {
  private static final Logger LOG = Logger.getLogger(CacheCore.class.getName());

  // index id generators
  private static int componentInstanceCount = 0;
  private static int metricNameCount = 0;

  // index id map: componentName -(map)-&gt; instanceId -(map)-&gt; locator:int
  private final Map<String, Map<String, Integer>> idxComponentInstance;
  // index id map: metricName -(map)-&gt; locator:int
  private final Map<String, Integer> idxMetricName;

  // exception store: following component-instance hierarchy
  private final HashMap<Integer, LinkedList<ExceptionDatapoint>> cacheException;
  // metric store
  private final TreeMap<Long, Map<Long, LinkedList<MetricDatapoint>>> cacheMetric;

  // looper for purge
  private WakeableLooper looper = null;

  // metric clock: rotate bucket, in milliseconds
  private final Duration maxInterval;
  private final Duration interval;
  // exception limit
  private final long maxExceptionCount;
  private final Ticker ticker;

  /**
   * constructor: CacheCore needs two intervals to configure metrics time window
   * and one number to limit exception count
   *
   * @param maxInterval metric: cache how long time?
   * @param interval metric: purge how often?
   * @param maxException exception: cache how many?
   */
  public CacheCore(Duration maxInterval, Duration interval, long maxException) {
    this(maxInterval, interval, maxException, new Ticker());
  }

  @VisibleForTesting
  CacheCore(Duration maxInterval, Duration interval, long maxException, Ticker ticker) {
    this.maxInterval = maxInterval;
    this.interval = interval;
    this.maxExceptionCount = maxException;
    this.ticker = ticker;

    cacheException = new HashMap<>();
    cacheMetric = new TreeMap<>();
    long now = ticker.read();
    for (long i = now - this.maxInterval.toMillis(); i < now; i += this.interval.toMillis()) {
      cacheMetric.put(i, new HashMap<Long, LinkedList<MetricDatapoint>>());
    }

    idxComponentInstance = new HashMap<>();
    idxMetricName = new HashMap<>();
  }

  private void assureComponentInstance(String componentName, String instanceId) {
    if (!idxComponentInstance.containsKey(componentName)) {
      idxComponentInstance.put(componentName, new HashMap<String, Integer>());
    }
    Map<String, Integer> map = idxComponentInstance.get(componentName);
    if (!map.containsKey(instanceId)) {
      map.put(instanceId, componentInstanceCount++);
    }
  }

  public boolean componentInstanceExists(String componentName, String instanceId) {
    if (componentName != null
        && !idxComponentInstance.containsKey(componentName)) {
      return false;
    }
    if (instanceId != null
        && !idxComponentInstance.get(componentName).containsKey(instanceId)) {
      return false;
    }
    return true;
  }

  public boolean metricExists(String name) {
    return idxMetricName.containsKey(name);
  }

  private void assureMetricName(String name) {
    if (!idxMetricName.containsKey(name)) {
      idxMetricName.put(name, metricNameCount++);
    }
  }

  /**
   * compatible with heron::tmanager::TMetricsCollector
   * @param metrics The metrics to be added
   */
  public void addMetricException(TopologyManager.PublishMetrics metrics) {
    synchronized (CacheCore.class) {
      for (TopologyManager.MetricDatum metricDatum : metrics.getMetricsList()) {
        addMetric(metricDatum);
      }
      for (TopologyManager.TmanagerExceptionLog exceptionLog : metrics.getExceptionsList()) {
        addException(exceptionLog);
      }
    }
  }

  /**
   * Make metric bucket id
   * Bucket id is made of two integers:
   * 1. the higher part is from idxComponentInstance locator:int
   * 2. the lower part is from idxMetricName locator:int
   * The metric bucket id is considered as union hash index of <component-instance, metricName>
   *
   * @param hi index of [component-instance]
   * @param lo index of metric name
   * @return bucket id
   */
  private long makeBucketId(int hi, int lo) {
    return (((long) hi) << 32) | (lo & 0xffffffffL);
  }

  /**
   * The 'cacheMetric' is a tree map organized by timestamp.
   * The key indicates the startTime of the time window.
   * <p>
   * The insertion procedure:
   * 1. find the leaf according to the metric timestamp. TreeMap.floorEntry finds the leaf whose
   * time window contains the given timestamp
   * 2. if the leaf is null, a new bucket is created; else insert into existing bucket.
   *
   * @param metricDatum the metric to be inserted
   */
  private void addMetric(TopologyManager.MetricDatum metricDatum) {
    String componentName = metricDatum.getComponentName();
    String instanceId = metricDatum.getInstanceId();
    String metricName = metricDatum.getName();

    assureComponentInstance(componentName, instanceId);
    assureMetricName(metricName);
    // calc bucket idx
    int idx1 = idxComponentInstance.get(componentName).get(instanceId);
    int idx2 = idxMetricName.get(metricName);
    long bucketId = makeBucketId(idx1, idx2);

    // fetch the bucket
    Map.Entry<Long, Map<Long, LinkedList<MetricDatapoint>>> entry =
        cacheMetric.floorEntry(metricDatum.getTimestamp());
    if (entry != null) {
      Map<Long, LinkedList<MetricDatapoint>> locator = entry.getValue();
      if (!locator.containsKey(bucketId)) {
        locator.put(bucketId, new LinkedList<MetricDatapoint>());
      }
      LinkedList<MetricDatapoint> bucket = locator.get(bucketId);
      // store the metric
      MetricDatapoint datum =
          new MetricDatapoint(metricDatum.getTimestamp(), metricDatum.getValue());
      bucket.offerFirst(datum);
    } else {
      LOG.warning("too old metric, out of cache timestamp window, drop it: " + metricDatum);
    }
  }

  private void addException(TopologyManager.TmanagerExceptionLog exceptionLog) {
    String componentName = exceptionLog.getComponentName();
    String instanceId = exceptionLog.getInstanceId();
    assureComponentInstance(componentName, instanceId);
    // get exception idx
    int idx = idxComponentInstance.get(componentName).get(instanceId);
    // fetch the bucket
    if (!cacheException.containsKey(idx)) {
      cacheException.put(idx, new LinkedList<ExceptionDatapoint>());
    }
    LinkedList<ExceptionDatapoint> bucket = cacheException.get(idx);
    // store the exception
    ExceptionDatapoint e = new ExceptionDatapoint(exceptionLog.getHostname(),
        exceptionLog.getStacktrace(), exceptionLog.getLasttime(), exceptionLog.getFirsttime(),
        exceptionLog.getCount(), exceptionLog.getLogging());
    bucket.offerFirst(e);
    // purge
    while (bucket.size() > maxExceptionCount) {
      LOG.warning("too many exception, reach exception cache size cap, drop it: " + exceptionLog);
      bucket.pollLast();
    }
  }

  /**
   * for internal process use
   *
   * @param request <p>
   * idxMetricName == null: query all metrics
   * idxMetricName == []: query none metric
   * idxMetricName == [a, b, c .. ]: query metric a, b and c, ..
   * <p>
   * idxComponentInstance == null: query all components
   * idxComponentInstance == []: query none component
   * idxComponentInstance == [c1-&gt;null, ..]: query all instances of c1, ..
   * idxComponentInstance == [c1-&gt;[], ..]: query none instance of c1, ..
   * idxComponentInstance == [c1-&gt;[a, b, c, ..], ..]: query instance a, b, c, .. of c1, ..
   * <p>
   * assert: startTime &lt;= endTime
   *
   * @param metricNameType map: metric name to type
   *
   * @return query result
   */
  public MetricResponse getMetrics(
      MetricRequest request, MetricsFilter metricNameType) {
    LOG.fine("received query: " + request.toString());
    synchronized (CacheCore.class) {
      List<MetricDatum> response = new LinkedList<>();

      // candidate metric names
      Set<String> metricNameFilter = request.getMetricNames();
      if (metricNameFilter == null) {
        metricNameFilter = idxMetricName.keySet();
      }

      // candidate component names
      Map<String, Set<String>> componentInstanceMap = request.getComponentNameInstanceId();
      Set<String> componentNameFilter;
      if (componentInstanceMap == null) {
        componentNameFilter = idxComponentInstance.keySet();
      } else {
        componentNameFilter = componentInstanceMap.keySet();
      }

      for (String metricName : metricNameFilter) {
        if (!metricExists(metricName)) {
          continue;
        }
        MetricsFilter.MetricAggregationType type = metricNameType.getAggregationType(metricName);
        for (String componentName : componentNameFilter) {
          // candidate instance ids
          Set<String> instanceIdFilter;
          if (componentInstanceMap == null
              || componentInstanceMap.get(componentName) == null) {
            instanceIdFilter = idxComponentInstance.get(componentName).keySet();
          } else {
            instanceIdFilter = componentInstanceMap.get(componentName);
          }

          for (String instanceId : instanceIdFilter) {
            LOG.fine(componentName + "; " + instanceId + "; " + metricName + "; " + type);
            // get bucket_id
            int idx1 = idxComponentInstance.get(componentName).get(instanceId);
            int idx2 = idxMetricName.get(metricName);
            long bucketId = makeBucketId(idx1, idx2);

            // iterate buckets: the result may be empty due to the bucketId/hash filter
            List<MetricTimeRangeValue> metricValue = new LinkedList<>();
            switch (request.getAggregationGranularity()) {
              case AGGREGATE_ALL_METRICS:
              case AGGREGATE_BY_BUCKET:
                getAggregatedMetrics(metricValue,
                    request.getStartTime()/*when*/, request.getEndTime()/*when*/,
                    bucketId/*where*/, type/*how*/, request.getAggregationGranularity());
                break;
              case RAW:
                getRawMetrics(metricValue,
                    request.getStartTime(), request.getEndTime(), bucketId, type);
                break;
              default:
                LOG.warning("unknown aggregationGranularity type "
                    + request.getAggregationGranularity());
            }

            // make metric list in response
            response.add(new MetricDatum(componentName, instanceId, metricName, metricValue));
          } // end for: instance
        } // end for: component
      } // end for: metric
      return new MetricResponse(response);
    }
  }

  private void getRawMetrics(List<MetricTimeRangeValue> metricValue,
                             long startTime, long endTime, long bucketId,
                             MetricsFilter.MetricAggregationType type) {
    LOG.fine("getRawMetrics " + startTime + " " + endTime);
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey();
         key != null && key <= endTime;
         key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      if (bucket != null) {
        for (MetricDatapoint datapoint : bucket) {
          if (datapoint.inRange(startTime, endTime)) {
            // per data point
            metricValue.add(new MetricTimeRangeValue(datapoint));
          }
        } // end bucket
      }

    } // end tree
  }

  // we assume the metric value is Double: compatible with tmanager
  @SuppressWarnings("fallthrough")
  private void getAggregatedMetrics(List<MetricTimeRangeValue> metricValue,
                                    long startTime, long endTime, long bucketId,
                                    MetricsFilter.MetricAggregationType type,
                                    MetricGranularity granularity) {
    LOG.fine("getAggregatedMetrics " + startTime + " " + endTime);
    // per request
    long outterCountAvg = 0;

    // prepare range value
    long outterStartTime = Long.MAX_VALUE;
    long outterEndTime = 0;
    String outterValue = null;

    double outterResult = 0;
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey();
         key != null && key <= endTime;
         key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      if (bucket != null) {
        // per bucket
        long innerCountAvg = 0;

        // prepare range value
        long innerStartTime = Long.MAX_VALUE;
        long innerEndTime = 0;
        String innerValue = null;

        double innerResult = 0;
        for (MetricDatapoint datapoint : bucket) {
          if (datapoint.inRange(startTime, endTime)) {
            switch (type) {
              case AVG:
                outterCountAvg++;
                innerCountAvg++;
              case SUM:
                outterResult += Double.parseDouble(datapoint.getValue());
                innerResult += Double.parseDouble(datapoint.getValue());
                break;
              case LAST:
                if (outterEndTime < datapoint.getTimestamp()) {
                  outterValue = datapoint.getValue();
                }
                if (innerEndTime < datapoint.getTimestamp()) {
                  innerValue = datapoint.getValue();
                }
                break;
              case UNKNOWN:
              default:
                LOG.warning(
                    "Unknown metric type, CacheCore does not know how to aggregate " + type);
                return;
            }
            outterStartTime = Math.min(outterStartTime, datapoint.getTimestamp());
            outterEndTime = Math.max(outterEndTime, datapoint.getTimestamp());
            innerStartTime = Math.min(innerStartTime, datapoint.getTimestamp());
            innerEndTime = Math.max(innerEndTime, datapoint.getTimestamp());
          }
        } // end bucket

        if (type.equals(MetricsFilter.MetricAggregationType.AVG) && innerCountAvg > 0) {
          innerValue = String.valueOf(innerResult / innerCountAvg);
        } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
          innerValue = String.valueOf(innerResult);
        }
        if (innerValue != null && granularity.equals(MetricGranularity.AGGREGATE_BY_BUCKET)) {
          metricValue.add(new MetricTimeRangeValue(innerStartTime, innerEndTime, innerValue));
        }
      }

    } // end tree

    if (type.equals(MetricsFilter.MetricAggregationType.AVG) && outterCountAvg > 0) {
      outterValue = String.valueOf(outterResult / outterCountAvg);
    } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
      outterValue = String.valueOf(outterResult);
    }
    if (outterValue != null && granularity.equals(MetricGranularity.AGGREGATE_ALL_METRICS)) {
      metricValue.add(new MetricTimeRangeValue(outterStartTime, outterEndTime, outterValue));
    }
  }

  /**
   * for internal process use
   *
   * @param request <p>
   * idxComponentInstance == null: query all components
   * idxComponentInstance == []: query none component
   * idxComponentInstance == [c1-&gt;null, ..]: query all instances of c1, ..
   * idxComponentInstance == [c1-&gt;[], ..]: query none instance of c1, ..
   * idxComponentInstance == [c1-&gt;[a, b, c, ..], ..]: query instance a, b, c, .. of c1, ..
   *
   * @return query result
   */
  public ExceptionResponse getExceptions(
      ExceptionRequest request) {
    synchronized (CacheCore.class) {
      List<ExceptionDatum> response = new ArrayList<>();

      Map<String, Set<String>> componentNameInstanceId = request.getComponentNameInstanceId();

      // candidate component names
      Set<String> componentNameFilter;
      if (componentNameInstanceId == null) {
        componentNameFilter = idxComponentInstance.keySet();
      } else {
        componentNameFilter = componentNameInstanceId.keySet();
      }

      for (String componentName : componentNameFilter) {
        // candidate instance ids
        Set<String> instanceIdFilter;
        if (componentNameInstanceId == null
            || componentNameInstanceId.get(componentName) == null) {
          instanceIdFilter = idxComponentInstance.get(componentName).keySet();
        } else {
          instanceIdFilter = componentNameInstanceId.get(componentName);
        }

        for (String instanceId : instanceIdFilter) {
          int idx = idxComponentInstance.get(componentName).get(instanceId);
          for (ExceptionDatapoint exceptionDatapoint : cacheException.get(idx)) {
            response.add(new ExceptionDatum(componentName, instanceId, exceptionDatapoint));
          }
        }
      }

      return new ExceptionResponse(response);
    }
  }

  public void purge() {
    long now = ticker.read();
    synchronized (CacheCore.class) {
      // remove old
      while (!cacheMetric.isEmpty()) {
        Long firstKey = cacheMetric.firstKey();
        if (firstKey >= now - maxInterval.toMillis()) {
          break;
        }
        cacheMetric.remove(firstKey);
      }
      // add new
      cacheMetric.put(now, new HashMap<Long, LinkedList<MetricDatapoint>>());
      // next timer task
      if (looper != null) {
        looper.registerTimerEvent(interval, new Runnable() {
          @Override
          public void run() {
            purge();
          }
        });
      }
    }
  }

  /**
   * start purge looper task
   * @param wakeableLooper the looper to run timer
   */
  public void startPurge(WakeableLooper wakeableLooper) {
    synchronized (CacheCore.class) {
      if (looper == null) {
        looper = wakeableLooper;
      }

      looper.registerTimerEvent(interval, new Runnable() {
        @Override
        public void run() {
          purge();
        }
      });
    }
  }

  /**
   * stop metric purge looper
   */
  public void stopPurge() {
    synchronized (CacheCore.class) {
      if (looper != null) {
        looper = null;
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    for (Long k = cacheMetric.firstKey(); k != null; k = cacheMetric.higherKey(k)) {
      sb.append("[").append(k).append(":");
      for (Long idx : cacheMetric.get(k).keySet()) {
        sb.append("<").append(Long.toHexString(idx)).append("->");
        for (MetricDatapoint dp : cacheMetric.get(k).get(idx)) {
          sb.append(dp.toString());
        }
        sb.append(">");
      }
      sb.append("]");
    }
    sb.append("}");
    return sb.toString();
  }

  static class Ticker {
    long read() {
      return System.currentTimeMillis();
    }
  }
}
