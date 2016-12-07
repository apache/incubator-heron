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


package com.twitter.heron.slamgr;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricDatum;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricRequest;
import com.twitter.heron.proto.tmaster.TopologyMaster.MetricResponse;
import com.twitter.heron.proto.tmaster.TopologyMaster.PublishMetrics;


public class MetricCache {
  private static final Logger LOG = Logger.getLogger(MetricCache.class.getName());

  // map of component name to its metrics
  Map<String, ComponentMetrics> metrics_;
  int max_interval_;
  int nintervals_;
  int interval_;
  String metrics_sinks_yaml_;
  SLAMetrics tmetrics_info_;
  int start_time_;

  MetricCache(int _max_interval, String metrics_sinks_yaml) {
    max_interval_ = _max_interval;
    metrics_sinks_yaml_ = metrics_sinks_yaml;
    tmetrics_info_ = new SLAMetrics(metrics_sinks_yaml);
    start_time_ = (int) new Date().getTime();

    metrics_ = new HashMap<>();
  }

  public void AddMetric(PublishMetrics _metrics) {
    for (int i = 0; i < _metrics.getMetricsCount(); ++i) {
      String component_name = _metrics.getMetrics(i).getComponentName();
      AddMetricsForComponent(component_name, _metrics.getMetrics(i));
    }
  }

  void AddMetricsForComponent(String component_name, MetricDatum metrics_data) {
    ComponentMetrics component_metrics = GetOrCreateComponentMetrics(component_name);
    String name = metrics_data.getName();
    SLAMetrics.MetricAggregationType type = tmetrics_info_.GetAggregationType(name);
    component_metrics.AddMetricForInstance(metrics_data.getInstanceId(), name, type,
        metrics_data.getValue());
  }

  ComponentMetrics GetOrCreateComponentMetrics(String component_name) {
    if (!metrics_.containsKey(component_name)) {
      metrics_.put(component_name, new ComponentMetrics(component_name, nintervals_, interval_));
    }
    return metrics_.get(component_name);
  }

  // Returns a new response to fetch metrics. The request gets propagated to Component's and
  // Instance's get metrics. Doesn't own Response.
  MetricResponse GetMetrics(MetricRequest _request, TopologyAPI.Topology _topology) {
    MetricResponse.Builder response_builder = MetricResponse.newBuilder();

    if (!metrics_.containsKey(_request.getComponentName())) {
      boolean component_exists = false;
      for (int i = 0; i < _topology.getSpoutsCount(); i++) {
        if ((_topology.getSpouts(i)).getComp().getName() == _request.getComponentName()) {
          component_exists = true;
          break;
        }
      }
      if (!component_exists) {
        for (int i = 0; i < _topology.getBoltsCount(); i++) {
          if ((_topology.getBolts(i)).getComp().getName() == _request.getComponentName()) {
            component_exists = true;
            break;
          }
        }
      }
      if (component_exists) {
        LOG.log(Level.WARNING,
            "Metrics for component `" + _request.getComponentName() + "` are not available");
        response_builder.setStatus(response_builder.getStatusBuilder()
            .setStatus(Common.StatusCode.NOTOK)
            .setMessage("Metrics not available for component `" + _request.getComponentName() + "`")
            .build());
      } else {
        LOG.log(Level.SEVERE,
            "GetMetrics request received for unknown component " + _request.getComponentName());
        response_builder.setStatus(response_builder.getStatusBuilder()
            .setStatus(Common.StatusCode.NOTOK)
            .setMessage("Unknown component: " + _request.getComponentName())
            .build());
      }

    } else if (!_request.hasInterval() && !_request.hasExplicitInterval()) {
      LOG.log(Level.SEVERE,
          "GetMetrics request does not have either interval" + " nor explicit interval");
      response_builder.setStatus(response_builder.getStatusBuilder()
          .setStatus(Common.StatusCode.NOTOK)
          .setMessage("No interval or explicit interval set")
          .build());
    } else {
      long start_time, end_time;
      if (_request.hasInterval()) {
        end_time = new Date().getTime();
        if (_request.getInterval() <= 0) {
          start_time = 0;
        } else {
          start_time = end_time - _request.getInterval();
        }
      } else {
        start_time = _request.getExplicitInterval().getStart();
        end_time = _request.getExplicitInterval().getEnd();
      }
      metrics_.get(_request.getComponentName())
          .GetMetrics(_request, start_time, end_time, response_builder);
      response_builder.setInterval(end_time - start_time);
    }

    return response_builder.build();
  }

  public void Purge() {
    for (ComponentMetrics cm : metrics_.values()) {
      cm.Purge();
    }
  }

  // Timeseries of metrics.
  class TimeBucket {
    // A list of metrics each accumulated inside the time
    // that this bucket represents
    public LinkedList<String> data_;
    // Whats the start and end time that this TimeBucket contains metrics for
    public int start_time_;
    public int end_time_;

    TimeBucket(int bucket_interval) {
      start_time_ = (int) new Date().getTime() / 1000;
      end_time_ = start_time_ + bucket_interval;

      data_ = new LinkedList<>();
    }

    boolean overlaps(long start_time, long end_time) {
      return start_time_ <= end_time && start_time <= end_time_;
    }

    double aggregate() {
      if (data_.isEmpty()) {
        return 0;
      } else {
        double total = 0;
        for (String s : data_) {
          total += Double.parseDouble(s);
        }
        return total;
      }
    }

    long count() {
      return data_.size();
    }
  }

  // Data structure to store metrics. A metric is a Time series of data.
  class Metric {
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
    public MetricResponse.IndividualMetric GetMetrics(boolean minutely,
                                                      long start_time, long end_time) {
      MetricResponse.IndividualMetric.Builder response_builder
          = MetricResponse.IndividualMetric.newBuilder();
      response_builder.setName(name_);

      if (minutely) {
        // we need minutely data
        for (TimeBucket bucket : data_) {
          // Does this time bucket have overlap with needed range
          if (bucket.overlaps(start_time, end_time)) {
            MetricResponse.IndividualMetric.IntervalValue.Builder val_builder =
                MetricResponse.IndividualMetric.IntervalValue.newBuilder();

            TopologyMaster.MetricInterval mi = val_builder.getIntervalBuilder()
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
  }

  // Most granualar metrics/exception store level. This store exception and metrics
  // associated with an instance.
  class InstanceMetrics {
    private String instance_id_;
    private int nbuckets_;
    private int bucket_interval_;
    // map between metric name and its values
    private Map<String, Metric> metrics_;
    // list of exceptions
    private List<TopologyMaster.TmasterExceptionLog> exceptions_;

    // ctor. '_instance_id' is the id generated by heron. '_nbuckets' number of metrics buckets
    // stored for instances belonging to this component.
    public InstanceMetrics(String instance_id, int nbuckets, int bucket_interval) {
      instance_id_ = instance_id;
      nbuckets_ = nbuckets;
      bucket_interval_ = bucket_interval;

      metrics_ = new HashMap<>();
    }

    // Clear old metrics associated with this instance.
    public void Purge() {
      for (Metric m : metrics_.values()) {
        m.Purge();
      }
    }

    // Add metrics with name '_name' of type '_type' and value _value.
    public void AddMetricWithName(String name, SLAMetrics.MetricAggregationType type,
                                  String value) {
      Metric metric_data = GetOrCreateMetric(name, type);
      metric_data.AddValueToMetric(value);
    }

    // Returns the metric metrics. Doesn't own _response.
    public void GetMetrics(MetricRequest request, long start_time, long end_time,
                           MetricResponse.Builder builder) {
      MetricResponse.TaskMetric.Builder tm = MetricResponse.TaskMetric.newBuilder();

      tm.setInstanceId(instance_id_);
      for (int i = 0; i < request.getMetricCount(); ++i) {
        String id = request.getMetric(i);
        if (metrics_.containsKey(id)) {
          TopologyMaster.MetricResponse.IndividualMetric im =
              metrics_.get(id).GetMetrics(request.getMinutely(), start_time, end_time);
          tm.addMetric(im);
        }
      }

      builder.addMetric(tm);
    }

    // Create or return existing Metric. Retains ownership of Metric object returned.
    private Metric GetOrCreateMetric(String name, SLAMetrics.MetricAggregationType type) {
      if (!metrics_.containsKey(name)) {
        metrics_.put(name, new Metric(name, type, nbuckets_, bucket_interval_));
      }
      return metrics_.get(name);
    }
  }

  // Component level metrics. A component metrics is a map storing metrics for each of its
  // instance as 'InstanceMetrics'.
  class ComponentMetrics {
    private String component_name_;
    private int nbuckets_;
    private int bucket_interval_;
    // map between instance id and its set of metrics
    private Map<String, InstanceMetrics> metrics_;

    // ctor. '_component_name' is the user supplied name given to the spout/bolt. '_nbuckets' is
    // number of buckets stored for this component.
    public ComponentMetrics(String component_name, int nbuckets, int bucket_interval) {
      component_name_ = component_name;
      nbuckets_ = nbuckets;
      bucket_interval_ = bucket_interval;

      metrics_ = new HashMap<>();
    }

    // Remove old metrics and exception associated with this spout/bolt component.
    public void Purge() {
      for (InstanceMetrics im : metrics_.values()) {
        im.Purge();
      }
    }

    // Add metrics for an Instance 'instance_id' of this spout/bolt component.
    public void AddMetricForInstance(String instance_id, String name,
                                     SLAMetrics.MetricAggregationType type, String value) {
      InstanceMetrics instance_metrics = GetOrCreateInstanceMetrics(instance_id);
      instance_metrics.AddMetricWithName(name, type, value);
    }

    // Request aggregated metrics for this component for the '_nbucket' interval.
    // Doesn't own '_response' object.
    public void GetMetrics(MetricRequest _request, long start_time, long end_time,
                           MetricResponse.Builder _response_builder) {
      if (_request.getInstanceIdCount() == 0) {
        // This means that all instances need to be returned
        for (InstanceMetrics im : metrics_.values()) {
          im.GetMetrics(_request, start_time, end_time, _response_builder);
          if (_response_builder.getStatus().getStatus() != Common.StatusCode.OK) {
            return;
          }
        }
      } else {
        for (int i = 0; i < _request.getInstanceIdCount(); ++i) {
          String id = _request.getInstanceId(i);
          if (!metrics_.containsKey(id)) {
            LOG.log(Level.SEVERE, "GetMetrics request received for unknown instance_id " + id);
            _response_builder.setStatus(_response_builder.getStatusBuilder()
                .setStatus(Common.StatusCode.NOTOK).build());
            return;
          } else {
            metrics_.get(id).GetMetrics(_request, start_time, end_time, _response_builder);
            if (_response_builder.getStatus().getStatus() != Common.StatusCode.OK) {
              return;
            }
          }
        }
      }
      _response_builder.setStatus(_response_builder.getStatusBuilder()
          .setStatus(Common.StatusCode.OK).build());
    }


    // Create or return existing mutable InstanceMetrics associated with 'instance_id'. This
    // method doesn't verify if the instance_id is valid fof the component.
    // Doesn't transfer ownership of returned InstanceMetrics.
    private InstanceMetrics GetOrCreateInstanceMetrics(String instance_id) {
      if (!metrics_.containsKey(instance_id)) {
        metrics_.put(instance_id, new InstanceMetrics(instance_id, nbuckets_, bucket_interval_));
      }
      return metrics_.get(instance_id);
    }
  }
}