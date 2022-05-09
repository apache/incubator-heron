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

#include "config/metrics-sinks-reader.h"
#include <list>
#include <string>
#include <utility>
#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/metrics-sinks-vars.h"

namespace heron {
namespace config {

MetricsSinksReader::MetricsSinksReader(std::shared_ptr<EventLoop> eventLoop,
        const sp_string& _defaults_file): YamlFileReader(eventLoop, _defaults_file) {
  LoadConfig();
}

MetricsSinksReader::~MetricsSinksReader() {}

void MetricsSinksReader::GetTManagerMetrics(std::list<std::pair<sp_string, sp_string> >& metrics) {
  if (config_[MetricsSinksVars::METRICS_SINKS_TMANAGER_SINK]) {
    YAML::Node n = config_[MetricsSinksVars::METRICS_SINKS_TMANAGER_SINK];
    if (n.IsMap() && n[MetricsSinksVars::METRICS_SINKS_TMANAGER_METRICS]) {
      YAML::Node m = n[MetricsSinksVars::METRICS_SINKS_TMANAGER_METRICS];
      if (m.IsMap()) {
        for (YAML::const_iterator it = m.begin(); it != m.end(); ++it) {
          metrics.push_back(make_pair(it->first.as<std::string>(), it->second.as<std::string>()));
        }
      }
    }
  }
}

void MetricsSinksReader::OnConfigFileLoad() {
  // Nothing really
}
}  // namespace config
}  // namespace heron
