#include "config/metrics-sinks-reader.h"

#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/metrics-sinks-vars.h"

namespace heron { namespace config {

MetricsSinksReader::MetricsSinksReader(EventLoop* eventLoop,
                                       const sp_string& _defaults_file)
  : YamlFileReader(eventLoop, _defaults_file)
{
  LoadConfig();
}

MetricsSinksReader::~MetricsSinksReader()
{
}

void MetricsSinksReader::GetTMasterMetrics(std::list<std::pair<sp_string, sp_string> >& metrics)
{
  if (config_[MetricsSinksVars::METRICS_SINKS_TMASTER_SINK]) {
    YAML::Node n = config_[MetricsSinksVars::METRICS_SINKS_TMASTER_SINK];
    if (n.IsMap() && n[MetricsSinksVars::METRICS_SINKS_TMASTER_METRICS]) {
      YAML::Node m = n[MetricsSinksVars::METRICS_SINKS_TMASTER_METRICS];
      if (m.IsMap()) {
        for(YAML::const_iterator it = m.begin(); it != m.end();++it) {
          metrics.push_back(make_pair(it->first.as<std::string>(),
                                      it->second.as<std::string>()));
        }
      }
    }
  }
}

void MetricsSinksReader::OnConfigFileLoad()
{
  // Nothing really
}

}} // end namespace
