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

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.metrics.MetricsFilter.MetricAggregationType;

// Data structure to store metrics. A metric is a Time series of data.
public class Metric {
  private static final Logger LOG = Logger.getLogger(Metric.class.getName());

  private String name;
  // Time series. data will be ordered by their time of arrival.
  private LinkedList<TimeBucket> data;
  // Type of metric. This can be SUM or AVG. It specify how to aggregate these metrics for
  // display.
  private MetricAggregationType metricType;
  private double allTimeCumulative;
  private long allTimeNitems;
  private int bucketInterval;

  public Metric(String name, MetricAggregationType type,
                int nbuckets, int bucketInterval) {
    this.name = name;
    metricType = type;
    allTimeCumulative = 0;
    allTimeNitems = 0;
    this.bucketInterval = bucketInterval;

    data = new LinkedList<>();
    for (int i = 0; i < nbuckets; ++i) {
      data.offerLast(new TimeBucket(bucketInterval));
    }
  }

  public void Purge() {
    data.pollLast();
    data.offerFirst(new TimeBucket(bucketInterval));
  }

  // Add a new value to the end of 'data' extending the time series.
  public void AddValueToMetric(String value) {
    if (metricType == MetricAggregationType.LAST) {
      // Just keep one value per time bucket
      data.peekFirst().data.clear();
      data.peekFirst().data.offerFirst(value);
      LOG.info("AddValueToMetric clear+offerFirst");
      // Do this for the cumulative as well
      allTimeCumulative = Double.parseDouble(value);
      allTimeNitems = 1;
    } else {
      data.peekFirst().data.offerFirst(value);
      LOG.info("AddValueToMetric offerFirst");
      allTimeCumulative += Double.parseDouble(value);
      allTimeNitems++;
    }
  }

  /**
   * Return  past '_nbuckets' value for this metric.
   *
   * @param minutely boolean-true will return metrics series in details
   * @param startTime in seconds
   * @param endTime in seconds
   * @return individual metric
   */
  public TopologyMaster.MetricResponse.IndividualMetric GetMetrics(boolean minutely,
                                                                   long startTime, long endTime) {
    TopologyMaster.MetricResponse.IndividualMetric.Builder responseBuilder
        = TopologyMaster.MetricResponse.IndividualMetric.newBuilder();
    responseBuilder.setName(name);

    if (minutely) {
      // we need minutely data
      for (TimeBucket bucket : data) {
        // Does this time bucket have overlap with needed range
        if (bucket.overlaps(startTime, endTime)) {
          TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.Builder valBuilder =
              TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

          TopologyMaster.MetricInterval mi = TopologyMaster.MetricInterval.newBuilder()
              .setStart(bucket.startTime).setEnd(bucket.endTime).build();
          valBuilder.setInterval(mi);

          double result = bucket.aggregate();
          if (metricType == MetricAggregationType.SUM) {
            valBuilder.setValue(String.valueOf(result));
          } else if (metricType == MetricAggregationType.AVG) {
            double avg = result / bucket.count();
            valBuilder.setValue(String.valueOf(avg));
          } else if (metricType == MetricAggregationType.LAST) {
            valBuilder.setValue(String.valueOf(result));
          } else {
            LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
          }

          responseBuilder.addIntervalValues(valBuilder.build());
        }
        // The timebuckets are reverse chronologically arranged
        if (startTime > bucket.endTime) {
          break;
        }
      }
    } else {
      // We don't need minutely data
      double result = 0;
      if (startTime <= 0) {
        // We want cumulative metrics
        if (metricType == MetricAggregationType.SUM) {
          result = allTimeCumulative;
        } else if (metricType == MetricAggregationType.AVG) {
          result = allTimeCumulative / allTimeNitems;
        } else if (metricType == MetricAggregationType.LAST) {
          result = allTimeCumulative;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
        }
      } else {
        // we want only for a specific interval
        long totalItems = 0;
        double totalCount = 0;
        for (TimeBucket bucket : data) {
          // Does this time bucket have overlap with needed range
          if (bucket.overlaps(startTime, endTime)) {
            totalCount += bucket.aggregate();
            totalItems += bucket.count();
            if (metricType == MetricAggregationType.LAST) {
              break;
            }
          }
          // The timebuckets are reverse chronologically arranged
          if (startTime > bucket.endTime) {
            break;
          }
        }
        if (metricType == MetricAggregationType.SUM) {
          result = totalCount;
        } else if (metricType == MetricAggregationType.AVG) {
          result = totalCount / totalItems;
        } else if (metricType == MetricAggregationType.LAST) {
          result = totalCount;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
        }
      }
      responseBuilder.setValue(String.valueOf(result));
    }

    return responseBuilder.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("m_name: " + name).append(", type: " + metricType)
        .append(", interval: " + bucketInterval).append(", nitems: " + allTimeNitems)
        .append(", cumulative: " + allTimeCumulative).append(", data");
    for (TimeBucket tb : data) {
      sb.append("\n => " + tb.toString());
    }
    sb.append(" $\n");
    return sb.toString();
  }

  /**
   * Return  past '_nbuckets' value for this metric.
   *
   * @param minutely boolean-true will return metrics series in details
   * @param startTime in seconds
   * @param endTime in seconds
   */
  public void GetMetrics(boolean minutely, long startTime, long endTime,
                         MetricsCacheQueryUtils.IndividualMetric responseBuilder) {

    responseBuilder.name = name;

    if (minutely) {
      // we need minutely data
      for (TimeBucket bucket : data) {
        // Does this time bucket have overlap with needed range
        if (bucket.overlaps(startTime, endTime)) {
          MetricsCacheQueryUtils.IntervalValue valBuilder =
              new MetricsCacheQueryUtils.IntervalValue();

          MetricsCacheQueryUtils.MetricInterval mi = new MetricsCacheQueryUtils.MetricInterval();
          mi.start = bucket.startTime;
          mi.end = bucket.endTime;
          valBuilder.interval = mi;

          double result = bucket.aggregate();
          if (metricType == MetricAggregationType.SUM) {
            valBuilder.value = String.valueOf(result);
          } else if (metricType == MetricAggregationType.AVG) {
            double avg = result / bucket.count();
            valBuilder.value = String.valueOf(avg);
          } else if (metricType == MetricAggregationType.LAST) {
            valBuilder.value = String.valueOf(result);
          } else {
            LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
          }

          responseBuilder.intervalValues.add(valBuilder);
        }
        // The timebuckets are reverse chronologically arranged
        if (startTime > bucket.endTime) {
          break;
        }
      }
    } else {
      // We don't need minutely data
      double result = 0;
      if (startTime <= 0) {
        // We want cumulative metrics
        if (metricType == MetricAggregationType.SUM) {
          result = allTimeCumulative;
        } else if (metricType == MetricAggregationType.AVG) {
          result = allTimeCumulative / allTimeNitems;
        } else if (metricType == MetricAggregationType.LAST) {
          result = allTimeCumulative;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
        }
      } else {
        // we want only for a specific interval
        long totalItems = 0;
        double totalCount = 0;
        for (TimeBucket bucket : data) {
          // Does this time bucket have overlap with needed range
          if (bucket.overlaps(startTime, endTime)) {
            totalCount += bucket.aggregate();
            totalItems += bucket.count();
            if (metricType == MetricAggregationType.LAST) {
              break;
            }
          }
          // The timebuckets are reverse chronologically arranged
          if (startTime > bucket.endTime) {
            break;
          }
        }
        if (metricType == MetricAggregationType.SUM) {
          result = totalCount;
        } else if (metricType == MetricAggregationType.AVG) {
          result = totalCount / totalItems;
        } else if (metricType == MetricAggregationType.LAST) {
          result = totalCount;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metricType);
        }
      }
      responseBuilder.value = String.valueOf(result);
    }
  }
}
