#include <iostream>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/metrics-sinks-reader.h"
#include "metrics/tmaster-metrics.h"

#include <list>

namespace heron { namespace common {

TMasterMetrics::TMasterMetrics(const sp_string& sinks_filename,
                               EventLoop* eventLoop)
{
  sinks_reader_ = new config::MetricsSinksReader(eventLoop, sinks_filename);
  std::list<std::pair<sp_string, sp_string> > metrics;
  sinks_reader_->GetTMasterMetrics(metrics);
  std::list<std::pair<sp_string, sp_string> >::iterator iter;
  for (iter = metrics.begin(); iter != metrics.end(); ++iter) {
    metrics_prefixes_[iter->first] = TranslateFromString(iter->second);
  }
}

TMasterMetrics::~TMasterMetrics()
{
  delete sinks_reader_;
}

bool TMasterMetrics::IsTMasterMetric(const sp_string& _name)
{
  std::map<sp_string, MetricAggregationType>::iterator iter;
  for (iter = metrics_prefixes_.begin(); iter != metrics_prefixes_.end(); ++iter) {
    if (_name.find(iter->first) == 0) return true;
  }
  return false;
}

TMasterMetrics::MetricAggregationType
TMasterMetrics::GetAggregationType(const sp_string& _name) {
  std::map<sp_string, MetricAggregationType>::iterator iter;
  for (iter = metrics_prefixes_.begin(); iter != metrics_prefixes_.end(); ++iter) {
    if (_name.find(iter->first) == 0) {
      return iter->second;
    }
  }
  return UNKNOWN;
}

TMasterMetrics::MetricAggregationType TMasterMetrics::TranslateFromString(const sp_string& type)
{
  if (type == "SUM") return SUM;
  else if (type == "AVG") return AVG;
  else if (type == "LAST") return LAST;
  else {
    LOG(FATAL) << "Unknown metrics type in metrics sinks " << type;
    return UNKNOWN;
  }
}

}} // end namespace
