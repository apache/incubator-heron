/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __TMETRICS_COLLECTOR_H_
#define __TMETRICS_COLLECTOR_H_

#include <map>
#include <list>
#include <string>
#include "basics/callback.h"
#include "basics/sptypes.h"
#include "network/event_loop.h"
#include "proto/tmaster.pb.h"
#include "proto/topology.pb.h"
#include "metrics/tmaster-metrics.h"

namespace heron {
namespace tmaster {
// Helper class to manage aggregation and and serving of metrics. Metrics are logically stored as a
// component_name -> {instance_id ->value}n .
// TODO(kramasamy): Store metrics persistently to prevent against crashes.
class TMetricsCollector {
 public:
  // _max_interval is how far along we keep individual metric blobs.
  TMetricsCollector(sp_int32 _max_interval, EventLoop* eventLoop,
                    const std::string& metrics_sinks_yaml);

  // Deletes all stored ComponentMetrics.
  virtual ~TMetricsCollector();

  // Initiated on recieving a new _metrics from metricsmanager. Will initiate appropriate calls
  // to add metrics/exception stored in '_metrics' to the respective components.
  void AddMetric(const proto::tmaster::PublishMetrics& _metrics);

  // Returns a new response to fetch metrics. The request gets propagated to Component's and
  // Instance's get metrics. Doesn't own Response.
  proto::tmaster::MetricResponse* GetMetrics(const proto::tmaster::MetricRequest& _request,
                                             const proto::api::Topology* _topology);

  // Returns response for fetching exceptions. Doesn't own response.
  proto::tmaster::ExceptionLogResponse* GetExceptions(
      const proto::tmaster::ExceptionLogRequest& request);

  // Returns exception summary response. Doesn't own response.
  proto::tmaster::ExceptionLogResponse* GetExceptionsSummary(
      const proto::tmaster::ExceptionLogRequest& request);

 private:
  // Fetches exceptions for ExceptionLogRequest. Save the returned exception in
  // 'all_exceptions'.
  //  Doesn't own 'all_exceptions' pointer
  void GetExceptionsHelper(const proto::tmaster::ExceptionLogRequest& request,
                           proto::tmaster::ExceptionLogResponse* all_excepions);

  // Aggregate exceptions from 'all_exceptions' to 'aggregate_exceptions'.
  // Doesn't own 'aggregate_exceptions'.
  void AggregateExceptions(const proto::tmaster::ExceptionLogResponse& all_exceptions,
                           proto::tmaster::ExceptionLogResponse* aggregate_exceptions);

  // Add metrics for 'component_name'
  void AddMetricsForComponent(const sp_string& component_name,
                              const proto::tmaster::MetricDatum& metrics_data);
  // Add exception logs for 'component_name'
  void AddExceptionsForComponent(const sp_string& component_name,
                                 const proto::tmaster::TmasterExceptionLog& exception_log);

  // Clean all metrics.
  void Purge(EventLoop::Status _status);

  // Timeseries of metrics.
  struct TimeBucket {
    // A list of metrics each accumulated inside the time
    // that this bucket represents
    std::list<sp_string> data_;
    // Whats the start and end time that this TimeBucket contains metrics for
    sp_int32 start_time_;
    sp_int32 end_time_;

    explicit TimeBucket(sp_int32 bucket_interval) {
      start_time_ = time(NULL);
      end_time_ = start_time_ + bucket_interval;
    }

    bool overlaps(sp_int64 start_time, sp_int64 end_time) {
      return start_time_ <= end_time && start_time <= end_time_;
    }

    sp_double64 aggregate() {
      if (data_.empty()) {
        return 0;
      } else {
        sp_double64 total = 0;
        for (std::list<sp_string>::iterator it = data_.begin(); it != data_.end(); ++it) {
          total += strtod(it->c_str(), NULL);
        }
        return total;
      }
    }

    sp_int64 count() { return data_.size(); }
  };

  // Data structure to store metrics. A metric is a Time series of data.
  // TODO(kramasamy): Use proto to store this data structure.
  class Metric {
   public:
    // TODO(kramasamy): Add ctor for default UNKNOWN type and give a set type function.
    Metric(const sp_string& name, common::TMasterMetrics::MetricAggregationType type,
           sp_int32 nbuckets, sp_int32 bucket_interval);

    // Deletes all TimeBucket.
    virtual ~Metric();

    void Purge();

    // Add a new value to the end of 'data_' extending the time series.
    void AddValueToMetric(const sp_string& value);

    // Return  past '_nbuckets' value for this metric.
    void GetMetrics(bool minutely, sp_int64 start_time, sp_int64 end_time,
                    proto::tmaster::MetricResponse::IndividualMetric* response);

   private:
    sp_string name_;
    // Time series. data_ will be ordered by their time of arrival.
    std::list<TimeBucket*> data_;
    // Type of metric. This can be SUM or AVG. It specify how to aggregate these metrics for
    // display.
    common::TMasterMetrics::MetricAggregationType metric_type_;

    sp_double64 all_time_cumulative_;

    sp_int64 all_time_nitems_;

    sp_int32 bucket_interval_;
  };

  // Most granualar metrics/exception store level. This store exception and metrics
  // associated with an instance.
  class InstanceMetrics {
   public:
    // ctor. '_instance_id' is the id generated by heron. '_nbuckets' number of metrics buckets
    // stored for instances belonging to this component.
    InstanceMetrics(const sp_string& instance_id, sp_int32 nbuckets, sp_int32 bucket_interval);
    // dtor
    virtual ~InstanceMetrics();

    // Clear old metrics associated with this instance.
    void Purge();

    // Add metrics with name '_name' of type '_type' and value _value.
    void AddMetricWithName(const sp_string& name,
                           common::TMasterMetrics::MetricAggregationType type,
                           const sp_string& value);

    // Add TmasterExceptionLog to the list of exceptions for this instance_id.
    void AddExceptions(const proto::tmaster::TmasterExceptionLog& exception);

    // Returns the metric metrics. Doesn't own _response.
    void GetMetrics(const proto::tmaster::MetricRequest& request, sp_int64 start_time,
                    sp_int64 end_time, proto::tmaster::MetricResponse* response);

    // Fills response for fetching exceptions. Doesn't own response.
    void GetExceptionLog(proto::tmaster::ExceptionLogResponse* response);

   private:
    // Create or return existing Metric. Retains ownership of Metric object returned.
    Metric* GetOrCreateMetric(const sp_string& name,
                              common::TMasterMetrics::MetricAggregationType type);

    sp_string instance_id_;
    sp_int32 nbuckets_;
    sp_int32 bucket_interval_;
    // map between metric name and its values
    std::map<sp_string, Metric*> metrics_;
    // list of exceptions
    std::list<proto::tmaster::TmasterExceptionLog*> exceptions_;
  };

  // Component level metrics. A component metrics is a map storing metrics for each of its
  // instance as 'InstanceMetrics'.
  class ComponentMetrics {
   public:
    // ctor. '_component_name' is the user supplied name given to the spout/bolt. '_nbuckets' is
    // number of buckets stored for this component.
    ComponentMetrics(const sp_string& component_name, sp_int32 nbuckets, sp_int32 bucket_interval);
    // dtor
    virtual ~ComponentMetrics();

    // Remove old metrics and exception associated with this spout/bolt component.
    void Purge();

    // Add metrics for an Instance 'instance_id' of this spout/bolt component.
    void AddMetricForInstance(const sp_string& instance_id, const sp_string& name,
                              common::TMasterMetrics::MetricAggregationType type,
                              const sp_string& value);
    // Add exception for an Instance 'instance_id' of this spout/bolt component.
    void AddExceptionForInstance(const sp_string& instance_id,
                                 const proto::tmaster::TmasterExceptionLog& exception);

    // Request aggregated metrics for this component for the '_nbucket' interval.
    // Doesn't own '_response' object.
    void GetMetrics(const proto::tmaster::MetricRequest& request, sp_int64 start_time,
                    sp_int64 end_time, proto::tmaster::MetricResponse* response);

    // Returns response for fetching exceptions. Doesn't own response.
    void GetExceptionsForInstance(const sp_string& instance_id,
                                  proto::tmaster::ExceptionLogResponse* response);

    void GetAllExceptions(proto::tmaster::ExceptionLogResponse* response);

   private:
    // Create or return existing mutable InstanceMetrics associated with 'instance_id'. This
    // method doesn't verify if the instance_id is valid fof the component.
    // Doesn't transfer ownership of returned InstanceMetrics.
    InstanceMetrics* GetOrCreateInstanceMetrics(const sp_string& instance_id);

    sp_string component_name_;
    sp_int32 nbuckets_;
    sp_int32 bucket_interval_;
    // map between instance id and its set of metrics
    std::map<sp_string, InstanceMetrics*> metrics_;
  };

  // Create or return existing mutable ComponentMetrics associated with 'component_name'.
  // Doesn't transfer ownership of returned ComponentMetrics
  ComponentMetrics* GetOrCreateComponentMetrics(const sp_string& component_name);

  // map of component name to its metrics
  std::map<sp_string, ComponentMetrics*> metrics_;
  sp_int32 max_interval_;
  sp_int32 nintervals_;
  sp_int32 interval_;
  EventLoop* eventLoop_;
  std::string metrics_sinks_yaml_;
  common::TMasterMetrics* tmetrics_info_;
  time_t start_time_;
};
}  // namespace tmaster
}  // namespace heron

#endif
