//  Copyright 2016 Twitter. All rights reserved.
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
package com.twitter.heron.metricscachemgr.metriccache;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.metricscachemgr.SLAMetrics;

// Data structure to store metrics. A metric is a Time series of data.
public class Metric {
  private static final Logger LOG = Logger.getLogger(Metric.class.getName());

  private String name_;
  // Time series. data_ will be ordered by their time of arrival.
  private LinkedList<TimeBucket> data_;
  // Type of metric. This can be SUM or AVG. It specify how to aggregate these metrics for
  // display.
  private SLAMetrics.MetricAggregationType metric_type_;
  private double all_time_cumulative_;
  private long all_time_nitems_;
  private int bucket_interval_;

  public Metric(String name, SLAMetrics.MetricAggregationType type,
                int nbuckets, int bucket_interval) {
    name_ = name;
    metric_type_ = type;
    all_time_cumulative_ = 0;
    all_time_nitems_ = 0;
    bucket_interval_ = bucket_interval;

    data_ = new LinkedList<>();
    for (int i = 0; i < nbuckets; ++i) {
      data_.offerLast(new TimeBucket(bucket_interval_));
    }
  }

  public void Purge() {
    data_.pollLast();
    data_.offerFirst(new TimeBucket(bucket_interval_));
  }

  // Add a new value to the end of 'data_' extending the time series.
  public void AddValueToMetric(String _value) {
    if (metric_type_ == SLAMetrics.MetricAggregationType.LAST) {
      // Just keep one value per time bucket
      data_.peekFirst().data_.clear();
      data_.peekFirst().data_.offerFirst(_value);
      // Do this for the cumulative as well
      all_time_cumulative_ = Double.parseDouble(_value);
      all_time_nitems_ = 1;
    } else {
      data_.peekFirst().data_.offerFirst(_value);
      all_time_cumulative_ += Double.parseDouble(_value);
      all_time_nitems_++;
    }
  }

  // Return  past '_nbuckets' value for this metric.
  public TopologyMaster.MetricResponse.IndividualMetric GetMetrics(boolean minutely,
                                                                   long start_time, long end_time) {
    TopologyMaster.MetricResponse.IndividualMetric.Builder response_builder
        = TopologyMaster.MetricResponse.IndividualMetric.newBuilder();
    response_builder.setName(name_);

    if (minutely) {
      // we need minutely data
      for (TimeBucket bucket : data_) {
        // Does this time bucket have overlap with needed range
        if (bucket.overlaps(start_time, end_time)) {
          TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.Builder val_builder =
              TopologyMaster.MetricResponse.IndividualMetric.IntervalValue.newBuilder();

          TopologyMaster.MetricInterval mi = TopologyMaster.MetricInterval.newBuilder()
              .setStart(bucket.start_time_).setEnd(bucket.end_time_).build();
          val_builder.setInterval(mi);

          double result = bucket.aggregate();
          if (metric_type_ == SLAMetrics.MetricAggregationType.SUM) {
            val_builder.setValue(String.valueOf(result));
          } else if (metric_type_ == SLAMetrics.MetricAggregationType.AVG) {
            double avg = result / bucket.count();
            val_builder.setValue(String.valueOf(avg));
          } else if (metric_type_ == SLAMetrics.MetricAggregationType.LAST) {
            val_builder.setValue(String.valueOf(result));
          } else {
            LOG.log(Level.SEVERE, "Unknown metric type " + metric_type_);
          }

          response_builder.addIntervalValues(val_builder.build());
        }
        // The timebuckets are reverse chronologically arranged
        if (start_time > bucket.end_time_) break;
      }
    } else {
      // We don't need minutely data
      double result = 0;
      if (start_time <= 0) {
        // We want cumulative metrics
        if (metric_type_ == SLAMetrics.MetricAggregationType.SUM) {
          result = all_time_cumulative_;
        } else if (metric_type_ == SLAMetrics.MetricAggregationType.AVG) {
          result = all_time_cumulative_ / all_time_nitems_;
        } else if (metric_type_ == SLAMetrics.MetricAggregationType.LAST) {
          result = all_time_cumulative_;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metric_type_);
        }
      } else {
        // we want only for a specific interval
        long total_items = 0;
        double total_count = 0;
        for (TimeBucket bucket : data_) {
          // Does this time bucket have overlap with needed range
          if (bucket.overlaps(start_time, end_time)) {
            total_count += bucket.aggregate();
            total_items += bucket.count();
            if (metric_type_ == SLAMetrics.MetricAggregationType.LAST) {
              break;
            }
          }
          // The timebuckets are reverse chronologically arranged
          if (start_time > bucket.end_time_) break;
        }
        if (metric_type_ == SLAMetrics.MetricAggregationType.SUM) {
          result = total_count;
        } else if (metric_type_ == SLAMetrics.MetricAggregationType.AVG) {
          result = total_count / total_items;
        } else if (metric_type_ == SLAMetrics.MetricAggregationType.LAST) {
          result = total_count;
        } else {
          LOG.log(Level.SEVERE, "Unknown metric type " + metric_type_);
        }
      }
      response_builder.setValue(String.valueOf(result));
    }

    return response_builder.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("m_name: " + name_).append(", type: " + metric_type_)
        .append(", interval: " + bucket_interval_).append(", nitems: " + all_time_nitems_)
        .append(", cumulative: " + all_time_cumulative_).append(", data");
    for (TimeBucket tb : data_) {
      sb.append("\n => " + tb.toString());
    }
    sb.append(" $\n");
    return sb.toString();
  }
}
