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
//  limitations under the License

package com.twitter.heron.metricscachemgr.metricscache;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.twitter.heron.metricscachemgr.metricscache.store.ExceptionDatapoint;
import com.twitter.heron.metricscachemgr.metricscache.store.MetricDatapoint;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * different from tmaster:
 * 1. free buckets for instances that are gone during scaling process
 * 2. time bucket by metric timestamp rather than metric message arriving time
 * 3. lock for multiple threads
 * same as tmaster:
 * 1. support same protobuf message/request format
 */
public class CacheCore {
  // logger
  private static final Logger LOG = Logger.getLogger(CacheCore.class.getName());

  // index id generators
  private static int componentInstanceCount = Integer.MIN_VALUE;
  private static int metricNameCount = Integer.MIN_VALUE;

  // timer for purge
  private Timer timer = null;

  // index id maps
  private Map<String, Map<String, Integer>> idxComponentInstance = null;
  private Map<String, Integer> idxMetricName = null;

  // exception store: following component-instance hierarchy
  private HashMap<Integer, LinkedList<ExceptionDatapoint>> cacheException;
  // metric store: timestamp_start -> [sparse: bucket_id -> bucket]
  // bucket_id is Long from idxComponentInstance Integer and idxMetricName Integer combined
  private TreeMap<Long, Map<Long, LinkedList<MetricDatapoint>>> cacheMetric;

  // metric clock: rotate bucket
  private int maxInterval;
  private int interval;
  // exception limit
  private int maxExceptionCount;

  /**
   * constructor
   *
   * @param maxInterval metric: cache how long time?
   * @param interval metric: purge how often?
   * @param maxException exception: cache how many?
   */
  public CacheCore(int maxInterval, int interval, int maxException) {
    this.maxInterval = maxInterval;
    this.interval = interval;
    this.maxExceptionCount = maxException;
    // cache
    cacheException = new HashMap<>();
    cacheMetric = new TreeMap<>();
    long now = Instant.now().getEpochSecond();
    for (long i = now - maxInterval; i < now; i += interval) {
      cacheMetric.put(i, new HashMap<>());
    }
  }

  private void AssureComponentInstance(String componentName, String instanceId) {
    if (!idxComponentInstance.containsKey(componentName)) {
      idxComponentInstance.put(componentName, new HashMap<>());
    }
    Map<String, Integer> map = idxComponentInstance.get(componentName);
    if (!map.containsKey(instanceId)) {
      map.put(instanceId, componentInstanceCount++);
    }
  }

  public boolean existComponentInstance(String componentName, String instanceId) {
    if (componentName!=null
        && !idxComponentInstance.containsKey(componentName)) {
      return false;
    }
    if (instanceId != null
        &&        !idxComponentInstance.get(componentName).containsKey(instanceId)) {
      return false;
    }
    return true;
  }

  private void AssureMetricName(String name) {
    if (!idxMetricName.containsKey(name)) {
      idxMetricName.put(name, metricNameCount++);
    }
  }

  /**
   * compatible with heron::tmaster::TMetricsCollector
   */
  public void AddMetricException(TopologyMaster.PublishMetrics metrics) {
    synchronized (CacheCore.class) {
      for (TopologyMaster.MetricDatum metricDatum : metrics.getMetricsList()) {
        AddMetric(metricDatum);
      }
      for (TopologyMaster.TmasterExceptionLog exceptionLog : metrics.getExceptionsList()) {
        AddException(exceptionLog);
      }
    }
  }

  // bucket id is Long: high bits = index component instance; low bits = index metric name
  private long makeBucketId(int idxComponentInstance, int idxMetricName) {
    long ret = idxComponentInstance;
    ret = ret << 32;
    ret |= idxMetricName;
    return ret;
  }

  private void AddMetric(TopologyMaster.MetricDatum metricDatum) {
    String componentName = metricDatum.getComponentName();
    String instanceId = metricDatum.getInstanceId();
    String metricName = metricDatum.getName();
    AssureComponentInstance(componentName, instanceId);
    AssureMetricName(metricName);
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
        locator.put(bucketId, new LinkedList<>());
      }
      LinkedList<MetricDatapoint> bucket = locator.get(bucketId);
      // store the metric
      MetricDatapoint datum = new MetricDatapoint();
      datum.timestamp = metricDatum.getTimestamp();
      datum.value = metricDatum.getValue();
      bucket.offerFirst(datum);
    } else {
      LOG.warning("too old metric: " + metricDatum);
    }
  }

  private void AddException(TopologyMaster.TmasterExceptionLog exceptionLog) {
    String componentName = exceptionLog.getComponentName();
    String instanceId = exceptionLog.getInstanceId();
    AssureComponentInstance(componentName, instanceId);
    // get exception idx
    int idx = idxComponentInstance.get(componentName).get(instanceId);
    // fetch the bucket
    LinkedList<ExceptionDatapoint> bucket = cacheException.get(idx);
    // store the exception
    ExceptionDatapoint e = new ExceptionDatapoint();
    e.componentName = exceptionLog.getComponentName();
    e.hostname = exceptionLog.getHostname();
    e.instanceId = exceptionLog.getInstanceId();
    e.stacktrace = exceptionLog.getStacktrace();
    e.lasttime = exceptionLog.getLasttime();
    e.firsttime = exceptionLog.getFirsttime();
    e.count = exceptionLog.getCount();
    e.logging = exceptionLog.getLogging();
    bucket.offerFirst(e);
    // purge
    while (bucket.size() > maxExceptionCount) {
      LOG.warning("too many exception: " + exceptionLog);
      bucket.pollLast();
    }
  }

  /**
   * for internal process use
   *
   * @param request idxMetricName == null: query all metrics
   * idxMetricName == []: query none metric
   * idxMetricName == [a, b, c .. ]: query metric a, b and c, ..
   * <p>
   * idxComponentInstance == null: query all components
   * idxComponentInstance == []: query none component
   * idxComponentInstance == [c1->null, ..]: query all instances of c1, ..
   * idxComponentInstance == [c1->[], ..]: query none instance of c1, ..
   * idxComponentInstance == [c1>[a, b, c, ..], ..]: query instance a, b, c, .. of c1, ..
   * <p>
   * assert: startTime <= endTime
   */
  public MetricsCacheQueryUtils.MetricResponse GetMetrics(
      MetricsCacheQueryUtils.MetricRequest request, MetricsFilter metricNameType) {
    synchronized (CacheCore.class) {
      MetricsCacheQueryUtils.MetricResponse response =
          new MetricsCacheQueryUtils.MetricResponse();
      response.metricList = new LinkedList<>();

      // candidate metric names
      Set<String> metricNameFilter;
      if (request.metricNames == null) { //
        metricNameFilter = idxMetricName.keySet();
      } else {
        metricNameFilter = request.metricNames;
      }

      // candidate component names
      Set<String> componentNameFilter;
      if (request.componentNameInstanceId == null) {
        componentNameFilter = idxComponentInstance.keySet();
      } else {
        componentNameFilter = request.componentNameInstanceId.keySet();
      }

      for (String metricName : metricNameFilter) {
        MetricsFilter.MetricAggregationType type = metricNameType.getAggregationType(metricName);
        for (String componentName : componentNameFilter) {
          // candidate instance ids
          Set<String> instanceIdFilter;
          if (request.componentNameInstanceId.get(componentName) == null) {
            instanceIdFilter = idxComponentInstance.get(componentName).keySet();
          } else {
            instanceIdFilter = request.componentNameInstanceId.get(componentName);
          }

          for (String instanceId : instanceIdFilter) {
            // get bucket_id
            int idx1 = idxComponentInstance.get(componentName).get(instanceId);
            int idx2 = idxMetricName.get(metricName);
            long bucketId = makeBucketId(idx1, idx2);

            // make metric list in response
            MetricsCacheQueryUtils.MetricDatum metricDatum =
                new MetricsCacheQueryUtils.MetricDatum();
            metricDatum.componentName = componentName;
            metricDatum.instanceId = instanceId;
            metricDatum.metricName = metricName;
            metricDatum.metricValue = new LinkedList<>();
            response.metricList.add(metricDatum);

            // iterate buckets
            switch (request.minutely) {
              case 0:
                GetAggregatedMetrics(metricDatum.metricValue,
                    request.startTime/*when*/, request.endTime/*when*/,
                    bucketId/*where*/, type/*how*/);
                break;
              case 1:
                GetMinuteMetrics(metricDatum.metricValue,
                    request.startTime, request.endTime, bucketId, type);
                break;
              case 2:
                GetRawMetrics(metricDatum.metricValue,
                    request.startTime, request.endTime, bucketId, type);
                break;
              default:
                LOG.warning("unknown minutely type " + request.minutely);
            }
          } // end for: instance
        } // end for: component
      } // end for: metric
      return response;
    }
  }

  private void GetRawMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                             long startTime, long endTime, long bucketId,
                             MetricsFilter.MetricAggregationType type) {
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey()
         ; key != null && key <= endTime
        ; key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      for (MetricDatapoint datapoint : bucket) {
        if (startTime <= datapoint.timestamp && datapoint.timestamp <= endTime) {
          // per data point
          MetricsCacheQueryUtils.MetricTimeRangeValue rangeValue =
              new MetricsCacheQueryUtils.MetricTimeRangeValue();
          rangeValue.startTime = datapoint.timestamp;
          rangeValue.endTime = datapoint.timestamp;
          rangeValue.value = datapoint.value;
          metricValue.add(rangeValue);
        }
      } // end bucket

    } // end tree
  }

  // we assume the metric value is Double: compatible with tmaster
  private void GetMinuteMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                                long startTime, long endTime, long bucketId,
                                MetricsFilter.MetricAggregationType type) {
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey()
         ; key != null && key <= endTime
        ; key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);
      // per bucket
      long countAvg = 0;

      MetricsCacheQueryUtils.MetricTimeRangeValue rangeValue =
          new MetricsCacheQueryUtils.MetricTimeRangeValue();
      rangeValue.startTime = Long.MAX_VALUE;
      rangeValue.endTime = 0;

      double result = 0;
      for (MetricDatapoint timestampDatum : bucket) {
        if (startTime <= timestampDatum.timestamp && timestampDatum.timestamp <= endTime) {
          switch (type) {
            case AVG:
              countAvg++;
            case SUM:
              result += Double.parseDouble(timestampDatum.value);
              break;
            case LAST:
              if (rangeValue.endTime < timestampDatum.timestamp) {
                rangeValue.value = timestampDatum.value;
              }
              break;
            case UNKNOWN:
            default:
              LOG.warning("Unknown metric type " + type);
              return;
          }
          rangeValue.startTime = Math.min(rangeValue.startTime, timestampDatum.timestamp);
          rangeValue.endTime = Math.max(rangeValue.endTime, timestampDatum.timestamp);
        }
      } // end bucket

      if (type.equals(MetricsFilter.MetricAggregationType.AVG)) {
        rangeValue.value = String.valueOf(result / countAvg);
      } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
        rangeValue.value = String.valueOf(result);
      }
      metricValue.add(rangeValue);
    } // end tree
  }

  // we assume the metric value is Double: compatible with tmaster
  private void GetAggregatedMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                                    long startTime, long endTime, long bucketId,
                                    MetricsFilter.MetricAggregationType type) {
    // per request
    long countAvg = 0;

    MetricsCacheQueryUtils.MetricTimeRangeValue rangeValue =
        new MetricsCacheQueryUtils.MetricTimeRangeValue();
    rangeValue.startTime = Long.MAX_VALUE;
    rangeValue.endTime = 0;

    double result = 0;
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey()
         ; key != null && key <= endTime
        ; key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      for (MetricDatapoint timestampDatum : bucket) {
        if (startTime <= timestampDatum.timestamp && timestampDatum.timestamp <= endTime) {
          switch (type) {
            case AVG:
              countAvg++;
            case SUM:
              result += Double.parseDouble(timestampDatum.value);
              break;
            case LAST:
              if (rangeValue.endTime < timestampDatum.timestamp) {
                rangeValue.value = timestampDatum.value;
              }
              break;
            case UNKNOWN:
            default:
              LOG.warning("Unknown metric type " + type);
              return;
          }
          rangeValue.startTime = Math.min(rangeValue.startTime, timestampDatum.timestamp);
          rangeValue.endTime = Math.max(rangeValue.endTime, timestampDatum.timestamp);
        }
      } // end bucket

    } // end tree

    if (type.equals(MetricsFilter.MetricAggregationType.AVG)) {
      rangeValue.value = String.valueOf(result / countAvg);
    } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
      rangeValue.value = String.valueOf(result);
    }
    metricValue.add(rangeValue);
  }

  /**
   * for internal process use
   */
  public MetricsCacheQueryUtils.ExceptionResponse GetExceptions(
      MetricsCacheQueryUtils.ExceptionRequest request) {
    synchronized (CacheCore.class) {
      MetricsCacheQueryUtils.ExceptionResponse response =
          new MetricsCacheQueryUtils.ExceptionResponse();

      for (Map.Entry<String, Set<String>> entry : request.componentNameInstanceId.entrySet()) {
        String componentName = entry.getKey();
        for (String instanceId : entry.getValue()) {
          int idx = idxComponentInstance.get(componentName).get(instanceId);
          response.exceptionDatapointList.addAll(cacheException.get(idx));
        }
      }

      return response;
    }
  }

  public void Purge() {
    long now = Instant.now().getEpochSecond();
    synchronized (CacheCore.class) {
      // remove old
      for (Long firstKey = cacheMetric.firstKey();
           firstKey != null && firstKey < now - maxInterval;
           firstKey = cacheMetric.firstKey()) {
        cacheMetric.remove(firstKey);
      }
      // add new
      cacheMetric.put(now, new HashMap<>());
    }
  }

  /**
   * start purge timer task
   */
  public void startPurge() {
    if (timer != null) {
      stopPurge();
    }

    timer = new Timer(true); // do not block exit
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        Purge();
      }
    }, 0, interval);
  }

  /**
   * stop metric purge timer
   */
  public void stopPurge() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
  }
}
