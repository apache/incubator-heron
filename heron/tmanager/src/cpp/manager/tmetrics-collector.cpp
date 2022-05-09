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

#include "manager/tmetrics-collector.h"
#include <iostream>
#include <list>
#include <map>
#include <string>
#include "metrics/tmanager-metrics.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "zookeeper/zkclient.h"
#include "proto/metrics.pb.h"
#include "proto/tmanager.pb.h"
#include "proto/topology.pb.h"
#include "config/heron-internals-config-reader.h"

namespace {
typedef heron::common::TManagerMetrics TManagerMetrics;
typedef heron::proto::tmanager::ExceptionLogRequest ExceptionLogRequest;
typedef heron::proto::tmanager::ExceptionLogResponse ExceptionLogResponse;
typedef heron::proto::tmanager::MetricRequest MetricRequest;
typedef heron::proto::tmanager::MetricResponse MetricResponse;
typedef heron::proto::tmanager::MetricResponse::IndividualMetric IndividualMetric;
typedef heron::proto::tmanager::MetricResponse::IndividualMetric::IntervalValue IntervalValue;
typedef heron::proto::tmanager::TmanagerExceptionLog TmanagerExceptionLog;
typedef heron::proto::tmanager::PublishMetrics PublishMetrics;
}  // namespace

namespace heron {
namespace tmanager {

TMetricsCollector::TMetricsCollector(sp_int32 _max_interval, std::shared_ptr<EventLoop> eventLoop,
                                     const std::string& metrics_sinks_yaml)
    : max_interval_(_max_interval),
      eventLoop_(eventLoop),
      metrics_sinks_yaml_(metrics_sinks_yaml),
      tmetrics_info_(make_unique<common::TManagerMetrics>(metrics_sinks_yaml, eventLoop)),
      start_time_(time(NULL)) {
  interval_ = config::HeronInternalsConfigReader::Instance()
                  ->GetHeronTmanagerMetricsCollectorPurgeIntervalSec();
  CHECK_EQ(max_interval_ % interval_, 0);
  nintervals_ = max_interval_ / interval_;
  auto cb = [this](EventLoop::Status status) { this->Purge(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, interval_ * 1000000), 0);
}

TMetricsCollector::~TMetricsCollector() {}

void TMetricsCollector::Purge(EventLoop::Status) {
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    iter->second->Purge();
  }
  auto cb = [this](EventLoop::Status status) { this->Purge(status); };

  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, interval_ * 1000000), 0);
}

void TMetricsCollector::AddMetricsForComponent(const sp_string& component_name,
                                               const proto::tmanager::MetricDatum& metrics_data) {
  auto component_metrics = GetOrCreateComponentMetrics(component_name);
  const sp_string& name = metrics_data.name();
  const TManagerMetrics::MetricAggregationType& type = tmetrics_info_->GetAggregationType(name);
  component_metrics->AddMetricForInstance(metrics_data.instance_id(), name, type,
                                          metrics_data.value());
}

void TMetricsCollector::AddExceptionsForComponent(const sp_string& component_name,
                                                  const TmanagerExceptionLog& exception_log) {
  auto component_metrics = GetOrCreateComponentMetrics(component_name);
  component_metrics->AddExceptionForInstance(exception_log.instance_id(), exception_log);
}

void TMetricsCollector::AddMetric(const PublishMetrics& _metrics) {
  for (sp_int32 i = 0; i < _metrics.metrics_size(); ++i) {
    const sp_string& component_name = _metrics.metrics(i).component_name();
    AddMetricsForComponent(component_name, _metrics.metrics(i));
  }
  for (int i = 0; i < _metrics.exceptions_size(); i++) {
    const sp_string& component_name = _metrics.exceptions(i).component_name();
    AddExceptionsForComponent(component_name, _metrics.exceptions(i));
  }
}

unique_ptr<MetricResponse> TMetricsCollector::GetMetrics(const MetricRequest& _request,
                                              const proto::api::Topology& _topology) {
  auto response = make_unique<MetricResponse>();

  if (metrics_.find(_request.component_name()) == metrics_.end()) {
    bool component_exists = false;
    for (int i = 0; i < _topology.spouts_size(); i++) {
      if ((_topology.spouts(i)).comp().name() == _request.component_name()) {
        component_exists = true;
        break;
      }
    }
    if (!component_exists) {
      for (int i = 0; i < _topology.bolts_size(); i++) {
        if ((_topology.bolts(i)).comp().name() == _request.component_name()) {
          component_exists = true;
          break;
        }
      }
    }
    if (component_exists) {
      LOG(WARNING) << "Metrics for component `" << _request.component_name()
                                                << "` are not available";
      response->mutable_status()->set_status(proto::system::NOTOK);
      response->mutable_status()->set_message("Metrics not available for component `" + \
                                              _request.component_name() + "`");
    } else {
      LOG(ERROR) << "GetMetrics request received for unknown component "
                 << _request.component_name();
      response->mutable_status()->set_status(proto::system::NOTOK);
      response->mutable_status()->set_message("Unknown component: " + _request.component_name());
    }

  } else if (!_request.has_interval() && !_request.has_explicit_interval()) {
    LOG(ERROR) << "GetMetrics request does not have either interval"
               << " nor explicit interval";
    response->mutable_status()->set_status(proto::system::NOTOK);
    response->mutable_status()->set_message("No interval or explicit interval set");
  } else {
    sp_int64 start_time, end_time;
    if (_request.has_interval()) {
      end_time = time(NULL);
      if (_request.interval() <= 0) {
        start_time = 0;
      } else {
        start_time = end_time - _request.interval();
      }
    } else {
      start_time = _request.explicit_interval().start();
      end_time = _request.explicit_interval().end();
    }
    metrics_[_request.component_name()]->GetMetrics(_request, start_time, end_time, *response);
    response->set_interval(end_time - start_time);
  }
  return response;
}

void TMetricsCollector::GetExceptionsHelper(const ExceptionLogRequest& request,
                                            ExceptionLogResponse& exceptions) {
  auto component_metrics = metrics_[request.component_name()];
  if (request.instances_size() == 0) {
    component_metrics->GetAllExceptions(exceptions);
  } else {
    for (int i = 0; i < request.instances_size(); ++i) {
      component_metrics->GetExceptionsForInstance(request.instances(i), exceptions);
    }
  }
}

unique_ptr<ExceptionLogResponse> TMetricsCollector::GetExceptions(
                                                            const ExceptionLogRequest& request) {
  auto response = make_unique<ExceptionLogResponse>();
  if (metrics_.find(request.component_name()) == metrics_.end()) {
    LOG(ERROR) << "GetExceptions request received for unknown component "
               << request.component_name();
    response->mutable_status()->set_status(proto::system::NOTOK);
    response->mutable_status()->set_message("Unknown component");
    return response;
  }
  response->mutable_status()->set_status(proto::system::OK);
  response->mutable_status()->set_message("OK");
  GetExceptionsHelper(request, *response);
  return response;
}

unique_ptr<ExceptionLogResponse> TMetricsCollector::GetExceptionsSummary(
                                                              const ExceptionLogRequest& request) {
  auto response = make_unique<ExceptionLogResponse>();

  if (metrics_.find(request.component_name()) == metrics_.end()) {
    LOG(ERROR) << "GetExceptionSummary request received for unknown component "
               << request.component_name();
    response->mutable_status()->set_status(proto::system::NOTOK);
    response->mutable_status()->set_message("Unknown component");
    return response;
  }
  response->mutable_status()->set_status(proto::system::OK);
  response->mutable_status()->set_message("OK");

  // Owns this pointer.
  auto all_exceptions = make_unique<ExceptionLogResponse>();
  GetExceptionsHelper(request, *all_exceptions);  // Store un aggregated exceptions.
  AggregateExceptions(*all_exceptions, *response);

  return response;
}

// Aggregate exceptions in all_exceptions  and fill up response
// (TODO: Merge aggregating exceptions based on classname and based on stack_trace (GetExceptions)
// into one function which take aggregation as argument. Modify the ExceptionRequest to
// take argument for which aggregation function to use)
void TMetricsCollector::AggregateExceptions(const ExceptionLogResponse& all_exceptions,
                                            ExceptionLogResponse& aggregate_exceptions) {
  using std::map;
  using std::string;

  map<string, unique_ptr<TmanagerExceptionLog>> exception_summary;  // Owns exception log pointer.
  for (int i = 0; i < all_exceptions.exceptions_size(); ++i) {
    const TmanagerExceptionLog& log = all_exceptions.exceptions(i);
    // Get classname by splitting on first colon
    const std::string& stack_trace = log.stacktrace();
    size_t pos = stack_trace.find_first_of(':');
    if (pos != std::string::npos) {
      const std::string class_name = stack_trace.substr(0, pos);
      if (exception_summary.find(class_name) == exception_summary.end()) {
        auto new_exception = make_unique<TmanagerExceptionLog>();
        new_exception->CopyFrom(log);
        new_exception->set_stacktrace(class_name);
        exception_summary[class_name] = std::move(new_exception);
      } else {
        TmanagerExceptionLog& prev_log = *exception_summary[class_name];
        prev_log.set_count(log.count() + prev_log.count());
        prev_log.set_lasttime(log.lasttime());
      }
    }
  }

  for (auto summary_iter = exception_summary.begin();
       summary_iter != exception_summary.end(); ++summary_iter) {
    aggregate_exceptions.add_exceptions()->CopyFrom(*(summary_iter->second));
  }
}

shared_ptr<TMetricsCollector::ComponentMetrics> TMetricsCollector::GetOrCreateComponentMetrics(
    const sp_string& component_name) {
  if (metrics_.find(component_name) == metrics_.end()) {
    metrics_[component_name] =
            std::make_shared<ComponentMetrics>(component_name, nintervals_, interval_);
  }

  return metrics_[component_name];
}

TMetricsCollector::ComponentMetrics::ComponentMetrics(const sp_string& component_name,
                                                      sp_int32 nbuckets, sp_int32 bucket_interval)
    : component_name_(component_name), nbuckets_(nbuckets), bucket_interval_(bucket_interval) {}

TMetricsCollector::ComponentMetrics::~ComponentMetrics() {}

void TMetricsCollector::ComponentMetrics::Purge() {
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    iter->second->Purge();
  }
}

void TMetricsCollector::ComponentMetrics::AddMetricForInstance(
    const sp_string& instance_id, const sp_string& name, TManagerMetrics::MetricAggregationType type,
    const sp_string& value) {
  auto instance_metrics = GetOrCreateInstanceMetrics(instance_id);
  instance_metrics->AddMetricWithName(name, type, value);
}

void TMetricsCollector::ComponentMetrics::AddExceptionForInstance(
    const sp_string& instance_id, const TmanagerExceptionLog& exception) {
  auto instance_metrics = GetOrCreateInstanceMetrics(instance_id);
  instance_metrics->AddExceptions(exception);
}

shared_ptr<TMetricsCollector::InstanceMetrics>
    TMetricsCollector::ComponentMetrics::GetOrCreateInstanceMetrics(const sp_string& instance_id) {
  if (metrics_.find(instance_id) == metrics_.end()) {
    metrics_[instance_id] =
            std::make_shared<InstanceMetrics>(instance_id, nbuckets_, bucket_interval_);
  }

  return metrics_[instance_id];
}

void TMetricsCollector::ComponentMetrics::GetMetrics(const MetricRequest& _request,
                                                     sp_int64 start_time, sp_int64 end_time,
                                                     MetricResponse& _response) {
  if (_request.instance_id_size() == 0) {
    // This means that all instances need to be returned
    for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
      iter->second->GetMetrics(_request, start_time, end_time, _response);
      if (_response.status().status() != proto::system::OK) {
        return;
      }
    }
  } else {
    for (sp_int32 i = 0; i < _request.instance_id_size(); ++i) {
      const sp_string& id = _request.instance_id(i);
      if (metrics_.find(id) == metrics_.end()) {
        LOG(ERROR) << "GetMetrics request received for unknown instance_id " << id;
        _response.mutable_status()->set_status(proto::system::NOTOK);
        return;
      } else {
        metrics_[id]->GetMetrics(_request, start_time, end_time, _response);
        if (_response.status().status() != proto::system::OK) {
          return;
        }
      }
    }
  }
  _response.mutable_status()->set_status(proto::system::OK);
}

void TMetricsCollector::ComponentMetrics::GetExceptionsForInstance(const sp_string& instance_id,
                                                                   ExceptionLogResponse& response) {
  if (metrics_.find(instance_id) != metrics_.end()) {
    metrics_[instance_id]->GetExceptionLog(response);
  }
}

void TMetricsCollector::ComponentMetrics::GetAllExceptions(ExceptionLogResponse& response) {
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    iter->second->GetExceptionLog(response);
  }
}

TMetricsCollector::InstanceMetrics::InstanceMetrics(const sp_string& instance_id, sp_int32 nbuckets,
                                                    sp_int32 bucket_interval)
    : instance_id_(instance_id), nbuckets_(nbuckets), bucket_interval_(bucket_interval) {}

TMetricsCollector::InstanceMetrics::~InstanceMetrics() {}

void TMetricsCollector::InstanceMetrics::Purge() {
  for (auto iter = metrics_.begin(); iter != metrics_.end(); ++iter) {
    iter->second->Purge();
  }
}

void TMetricsCollector::InstanceMetrics::AddMetricWithName(
    const sp_string& name, common::TManagerMetrics::MetricAggregationType type,
    const sp_string& value) {
  auto metric_data = GetOrCreateMetric(name, type);
  metric_data->AddValueToMetric(value);
}

// Creates a copy of exception and takes ownership of the pointer.
void TMetricsCollector::InstanceMetrics::AddExceptions(const TmanagerExceptionLog& exception) {
  // TODO(kramasamy): Aggregate exceptions across minutely buckets. Try to avoid duplication of
  // hash-fuction
  // used to aggregate in heron-worker.
  auto new_exception = make_unique<TmanagerExceptionLog>();
  new_exception->CopyFrom(exception);
  exceptions_.push_back(std::move(new_exception));
  sp_uint32 max_exception = config::HeronInternalsConfigReader::Instance()
                                ->GetHeronTmanagerMetricsCollectorMaximumException();
  while (exceptions_.size() > max_exception) {
    exceptions_.pop_front();
  }
}

shared_ptr<TMetricsCollector::Metric> TMetricsCollector::InstanceMetrics::GetOrCreateMetric(
    const sp_string& name, TManagerMetrics::MetricAggregationType type) {
  if (metrics_.find(name) == metrics_.end()) {
    metrics_[name] = std::make_shared<Metric>(name, type, nbuckets_, bucket_interval_);
  }
  return metrics_[name];
}

void TMetricsCollector::InstanceMetrics::GetMetrics(const MetricRequest& request,
                                                    sp_int64 start_time, sp_int64 end_time,
                                                    MetricResponse& response) {
  MetricResponse::TaskMetric* m = response.add_metric();
  m->set_instance_id(instance_id_);
  for (sp_int32 i = 0; i < request.metric_size(); ++i) {
    const sp_string& id = request.metric(i);
    if (metrics_.find(id) != metrics_.end()) {
      metrics_[id]->GetMetrics(request.minutely(), start_time, end_time, m->add_metric());
    }
  }
}

void TMetricsCollector::InstanceMetrics::GetExceptionLog(ExceptionLogResponse& response) {
  for (auto ex_iter = exceptions_.begin(); ex_iter != exceptions_.end(); ++ex_iter) {
    response.add_exceptions()->CopyFrom(*(*ex_iter));
  }
}

TMetricsCollector::Metric::Metric(const sp_string& name,
                                  common::TManagerMetrics::MetricAggregationType type,
                                  sp_int32 nbuckets, sp_int32 bucket_interval)
    : name_(name),
      metric_type_(type),
      all_time_cumulative_(0),
      all_time_nitems_(0),
      bucket_interval_(bucket_interval) {
  for (sp_int32 i = 0; i < nbuckets; ++i) {
    data_.push_back(make_unique<TimeBucket>(bucket_interval_));
  }
}

TMetricsCollector::Metric::~Metric() {}

void TMetricsCollector::Metric::Purge() {
  data_.pop_back();
  data_.push_front(make_unique<TimeBucket>(bucket_interval_));
}

void TMetricsCollector::Metric::AddValueToMetric(const sp_string& _value) {
  if (metric_type_ == common::TManagerMetrics::LAST) {
    // Just keep one value per time bucket
    data_.front()->data_.clear();
    data_.front()->data_.push_front(_value);
    // Do thsi for the cumulative as well
    all_time_cumulative_ = strtod(_value.c_str(), NULL);
    all_time_nitems_ = 1;
  } else {
    data_.front()->data_.push_front(_value);
    all_time_cumulative_ += strtod(_value.c_str(), NULL);
    all_time_nitems_++;
  }
}

void TMetricsCollector::Metric::GetMetrics(bool minutely, sp_int64 start_time, sp_int64 end_time,
                                           IndividualMetric* _response) {
  _response->set_name(name_);
  if (minutely) {
    // we need minutely data
    for (auto iter = data_.begin(); iter != data_.end(); ++iter) {
      TimeBucket& bucket = **iter;
      // Does this time bucket have overlap with needed range
      if (bucket.overlaps(start_time, end_time)) {
        IntervalValue* val = _response->add_interval_values();
        val->mutable_interval()->set_start(bucket.start_time_);
        val->mutable_interval()->set_end(bucket.end_time_);
        sp_double64 result = bucket.aggregate();
        if (metric_type_ == common::TManagerMetrics::SUM) {
          val->set_value(std::to_string(result));
        } else if (metric_type_ == common::TManagerMetrics::AVG) {
          sp_double64 avg = result / bucket.count();
          val->set_value(std::to_string(avg));
        } else if (metric_type_ == common::TManagerMetrics::LAST) {
          val->set_value(std::to_string(result));
        } else {
          LOG(FATAL) << "Unknown metric type " << metric_type_;
        }
      }
      // The timebuckets are reverse chronologically arranged
      if (start_time > bucket.end_time_) break;
    }
  } else {
    // We don't need minutely data
    sp_double64 result = 0;
    if (start_time <= 0) {
      // We want cumulative metrics
      if (metric_type_ == common::TManagerMetrics::SUM) {
        result = all_time_cumulative_;
      } else if (metric_type_ == common::TManagerMetrics::AVG) {
        result = all_time_cumulative_ / all_time_nitems_;
      } else if (metric_type_ == common::TManagerMetrics::LAST) {
        result = all_time_cumulative_;
      } else {
        LOG(FATAL) << "Uknown metric type " << metric_type_;
      }
    } else {
      // we want only for a specific interval
      sp_int64 total_items = 0;
      sp_double64 total_count = 0;
      for (auto iter = data_.begin(); iter != data_.end(); ++iter) {
        TimeBucket& bucket = **iter;
        // Does this time bucket have overlap with needed range
        if (bucket.overlaps(start_time, end_time)) {
          total_count += bucket.aggregate();
          total_items += bucket.count();
          if (metric_type_ == TManagerMetrics::LAST) break;
        }
        // The timebuckets are reverse chronologically arranged
        if (start_time > bucket.end_time_) break;
      }
      if (metric_type_ == TManagerMetrics::SUM) {
        result = total_count;
      } else if (metric_type_ == TManagerMetrics::AVG) {
        result = total_count / total_items;
      } else if (metric_type_ == TManagerMetrics::LAST) {
        result = total_count;
      } else {
        LOG(FATAL) << "Uknown metric type " << metric_type_;
      }
    }
    _response->set_value(std::to_string(result));
  }
}
}  // namespace tmanager
}  // namespace heron
