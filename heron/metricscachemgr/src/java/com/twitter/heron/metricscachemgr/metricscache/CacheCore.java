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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.metricscachemgr.metricscache.datapoint.ExceptionDatapoint;
import com.twitter.heron.metricscachemgr.metricscache.datapoint.MetricDatapoint;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter;

/**
 * different from tmaster:
 * 1. order bucket by metric timestamp rather than metric message arriving time
 * 2. free buckets for instances that are gone during scaling process
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

  // looper for purge
  private WakeableLooper looper = null;

  // index id maps
  private Map<String, Map<String, Integer>> idxComponentInstance = null;
  private Map<String, Integer> idxMetricName = null;

  // exception store: following component-instance hierarchy
  private HashMap<Integer, LinkedList<ExceptionDatapoint>> cacheException;
  // metric store: timestamp_start -> [sparse: bucket_id -> bucket]
  // bucket_id is Long from idxComponentInstance Integer and idxMetricName Integer combined
  private TreeMap<Long, Map<Long, LinkedList<MetricDatapoint>>> cacheMetric;

  // metric clock: rotate bucket, in milliseconds
  private long maxInterval;
  private long interval;
  // exception limit
  private long maxExceptionCount;

  /**
   * constructor
   *
   * @param maxInterval metric: cache how long time? in seconds
   * @param interval metric: purge how often? in seconds
   * @param maxException exception: cache how many?
   */
  public CacheCore(long maxInterval, long interval, long maxException) {
    this.maxInterval = maxInterval * 1000;
    this.interval = interval * 1000;
    this.maxExceptionCount = maxException;
    // cache
    cacheException = new HashMap<>();
    cacheMetric = new TreeMap<>();
    long now = System.currentTimeMillis();
    for (long i = now - this.maxInterval; i < now; i += this.interval) {
      cacheMetric.put(i, new HashMap<Long, LinkedList<MetricDatapoint>>());
    }
    // index
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

  public boolean existComponentInstance(String componentName, String instanceId) {
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

  public boolean existMetric(String name) {
    return idxMetricName.containsKey(name);
  }

  private void assureMetricName(String name) {
    if (!idxMetricName.containsKey(name)) {
      idxMetricName.put(name, metricNameCount++);
    }
  }

  /**
   * compatible with heron::tmaster::TMetricsCollector
   */
  public void addMetricException(TopologyMaster.PublishMetrics metrics) {
    synchronized (CacheCore.class) {
      for (TopologyMaster.MetricDatum metricDatum : metrics.getMetricsList()) {
        addMetric(metricDatum);
      }
      for (TopologyMaster.TmasterExceptionLog exceptionLog : metrics.getExceptionsList()) {
        addException(exceptionLog);
      }
    }
  }

  /**
   * make bucket id
   *
   * @param hi index of [component-instance]
   * @param lo index of metric name
   * @return bucket id
   */
  private long makeBucketId(int hi, int lo) {
    return (((long) hi) << 32) | (lo & 0xffffffffL);
  }

  private void addMetric(TopologyMaster.MetricDatum metricDatum) {
    String componentName = metricDatum.getComponentName();
    String instanceId = metricDatum.getInstanceId();
    String metricName = metricDatum.getName();
    LOG.info(componentName + " " + instanceId + " " + metricName);
    assureComponentInstance(componentName, instanceId);
    assureMetricName(metricName);
    // calc bucket idx
    int idx1 = idxComponentInstance.get(componentName).get(instanceId);
    int idx2 = idxMetricName.get(metricName);
    long bucketId = makeBucketId(idx1, idx2);
    LOG.info(Long.toHexString(bucketId) + " = "
        + Integer.toHexString(idx1) + " " + Integer.toHexString(idx2));
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
      MetricDatapoint datum = new MetricDatapoint();
      datum.timestamp = metricDatum.getTimestamp();
      datum.value = metricDatum.getValue();
      bucket.offerFirst(datum);
      LOG.info("addMetrics " + datum);
    } else {
      LOG.warning("too old metric: " + metricDatum);
    }
  }

  private void addException(TopologyMaster.TmasterExceptionLog exceptionLog) {
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
    ExceptionDatapoint e = new ExceptionDatapoint();
    e.componentName = exceptionLog.getComponentName();
    e.hostname = exceptionLog.getHostname();
    e.instanceId = exceptionLog.getInstanceId();
    e.stackTrace = exceptionLog.getStacktrace();
    e.lastTime = exceptionLog.getLasttime();
    e.firstTime = exceptionLog.getFirsttime();
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
  public MetricsCacheQueryUtils.MetricResponse getMetrics(
      MetricsCacheQueryUtils.MetricRequest request, MetricsFilter metricNameType) {
    LOG.info(request.toString());
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
        if (!existMetric(metricName)) {
          continue;
        }
        MetricsFilter.MetricAggregationType type = metricNameType.getAggregationType(metricName);
        for (String componentName : componentNameFilter) {
          // candidate instance ids
          Set<String> instanceIdFilter;
          if (request.componentNameInstanceId == null
              || request.componentNameInstanceId.get(componentName) == null) {
            instanceIdFilter = idxComponentInstance.get(componentName).keySet();
          } else {
            instanceIdFilter = request.componentNameInstanceId.get(componentName);
          }

          for (String instanceId : instanceIdFilter) {
            LOG.info(componentName + "; " + instanceId + "; " + metricName + "; " + type);
            // get bucket_id
            int idx1 = idxComponentInstance.get(componentName).get(instanceId);
            int idx2 = idxMetricName.get(metricName);
            long bucketId = makeBucketId(idx1, idx2);
            LOG.info(Long.toHexString(bucketId) + " = "
                + Integer.toHexString(idx1) + " " + Integer.toHexString(idx2));

            // make metric list in response
            MetricsCacheQueryUtils.MetricDatum metricDatum =
                new MetricsCacheQueryUtils.MetricDatum();
            metricDatum.componentName = componentName;
            metricDatum.instanceId = instanceId;
            metricDatum.metricName = metricName;
            metricDatum.metricValue = new LinkedList<>();
            response.metricList.add(metricDatum);

            // iterate buckets
            switch (request.aggregationGranularity) {
              case 0:
                getAggregatedMetrics(metricDatum.metricValue,
                    request.startTime/*when*/, request.endTime/*when*/,
                    bucketId/*where*/, type/*how*/);
                break;
              case 1:
                getMinuteMetrics(metricDatum.metricValue,
                    request.startTime, request.endTime, bucketId, type);
                break;
              case 2:
                getRawMetrics(metricDatum.metricValue,
                    request.startTime, request.endTime, bucketId, type);
                break;
              default:
                LOG.warning("unknown aggregationGranularity type "
                    + request.aggregationGranularity);
            }
          } // end for: instance
        } // end for: component
      } // end for: metric
      return response;
    }
  }

  private void getRawMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                             long startTime, long endTime, long bucketId,
                             MetricsFilter.MetricAggregationType type) {
    LOG.info("getRawMetrics " + startTime + " " + endTime);
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey();
         key != null && key <= endTime;
         key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      if (bucket != null) {
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
      }

    } // end tree
  }

  // we assume the metric value is Double: compatible with tmaster
  @SuppressWarnings("fallthrough")
  private void getMinuteMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                                long startTime, long endTime, long bucketId,
                                MetricsFilter.MetricAggregationType type) {
    LOG.info("getMinuteMetrics " + startTime + " " + endTime);
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey();
         key != null && key <= endTime;
         key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      if (bucket != null) {
        // per bucket
        long countAvg = 0;

        MetricsCacheQueryUtils.MetricTimeRangeValue rangeValue =
            new MetricsCacheQueryUtils.MetricTimeRangeValue();
        rangeValue.startTime = Long.MAX_VALUE;
        rangeValue.endTime = 0;

        double result = 0;
        for (MetricDatapoint datapoint : bucket) {
          if (startTime <= datapoint.timestamp && datapoint.timestamp <= endTime) {
            switch (type) {
              case AVG:
                countAvg++;
              case SUM:
                result += Double.parseDouble(datapoint.value);
                break;
              case LAST:
                if (rangeValue.endTime < datapoint.timestamp) {
                  rangeValue.value = datapoint.value;
                }
                break;
              case UNKNOWN:
              default:
                LOG.warning("Unknown metric type " + type);
                return;
            }
            rangeValue.startTime = Math.min(rangeValue.startTime, datapoint.timestamp);
            rangeValue.endTime = Math.max(rangeValue.endTime, datapoint.timestamp);
          }
        } // end bucket

        if (type.equals(MetricsFilter.MetricAggregationType.AVG)) {
          rangeValue.value = String.valueOf(result / countAvg);
        } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
          rangeValue.value = String.valueOf(result);
        }
        if (rangeValue.value != null) {
          metricValue.add(rangeValue);
        }
        LOG.info(rangeValue.toString());
      }

    } // end tree
  }

  // we assume the metric value is Double: compatible with tmaster
  @SuppressWarnings("fallthrough")
  private void getAggregatedMetrics(List<MetricsCacheQueryUtils.MetricTimeRangeValue> metricValue,
                                    long startTime, long endTime, long bucketId,
                                    MetricsFilter.MetricAggregationType type) {
    LOG.info("getAggregatedMetrics " + startTime + " " + endTime);
    // per request
    long countAvg = 0;

    MetricsCacheQueryUtils.MetricTimeRangeValue rangeValue =
        new MetricsCacheQueryUtils.MetricTimeRangeValue();
    rangeValue.startTime = Long.MAX_VALUE;
    rangeValue.endTime = 0;

    double result = 0;
    Long startKey = cacheMetric.floorKey(startTime);
    for (Long key = startKey != null ? startKey : cacheMetric.firstKey();
         key != null && key <= endTime;
         key = cacheMetric.higherKey(key)) {
      LinkedList<MetricDatapoint> bucket = cacheMetric.get(key).get(bucketId);

      if (bucket != null) {
        for (MetricDatapoint datapoint : bucket) {
          if (startTime <= datapoint.timestamp && datapoint.timestamp <= endTime) {
            switch (type) {
              case AVG:
                countAvg++;
              case SUM:
                result += Double.parseDouble(datapoint.value);
                break;
              case LAST:
                if (rangeValue.endTime < datapoint.timestamp) {
                  rangeValue.value = datapoint.value;
                }
                break;
              case UNKNOWN:
              default:
                LOG.warning("Unknown metric type " + type);
                return;
            }
            rangeValue.startTime = Math.min(rangeValue.startTime, datapoint.timestamp);
            rangeValue.endTime = Math.max(rangeValue.endTime, datapoint.timestamp);
          }
        } // end bucket
      }

    } // end tree

    if (type.equals(MetricsFilter.MetricAggregationType.AVG)) {
      rangeValue.value = String.valueOf(result / countAvg);
    } else if (type.equals(MetricsFilter.MetricAggregationType.SUM)) {
      rangeValue.value = String.valueOf(result);
    }
    if (rangeValue.value != null) {
      metricValue.add(rangeValue);
    }
    LOG.info(rangeValue.toString());
  }

  /**
   * for internal process use
   */
  public MetricsCacheQueryUtils.ExceptionResponse getExceptions(
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

  public void purge() {
    long now = System.currentTimeMillis();
    LOG.info("purge " + now);
    synchronized (CacheCore.class) {
      // remove old
      for (Long firstKey = cacheMetric.firstKey();
           firstKey != null && firstKey < now - maxInterval;
           firstKey = cacheMetric.firstKey()) {
        cacheMetric.remove(firstKey);
      }
      // add new
      cacheMetric.put(now, new HashMap<Long, LinkedList<MetricDatapoint>>());
      // next timer task
      if (looper != null) {
        looper.registerTimerEventInSeconds(interval, new Runnable() {
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
   */
  public void startPurge(WakeableLooper wakeableLooper) {
    synchronized (CacheCore.class) {
      if (looper == null) {
        looper = wakeableLooper;
      }

      looper.registerTimerEventInSeconds(interval, new Runnable() {
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
        sb.append("<").append(idx).append("->");
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
}
