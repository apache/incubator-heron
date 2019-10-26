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

#include "yaml-cpp/yaml.h"
#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"
#include "config/cluster-config-vars.h"
#include "config/cluster-config-reader.h"

namespace heron {
namespace config {

ClusterConfigReader::ClusterConfigReader(std::shared_ptr<EventLoop> eventLoop,
        const sp_string& _defaults_file): YamlFileReader(eventLoop, _defaults_file) {
  LoadConfig();
}

ClusterConfigReader::~ClusterConfigReader() {}

void ClusterConfigReader::FillClusterConfig(proto::api::Topology* _topology) {
  // Fill in the cluster config
  for (YAML::const_iterator iter = config_.begin(); iter != config_.end(); ++iter) {
    proto::api::Config::KeyValue* kv = _topology->mutable_topology_config()->add_kvs();
    kv->set_key(iter->first.as<sp_string>());
    kv->set_value(iter->second.as<sp_string>());
  }
}

void ClusterConfigReader::OnConfigFileLoad() {
  AddIfMissing(ClusterConfigVars::CLUSTER_METRICS_INTERVAL, "60");
}
}  // namespace config
}  // namespace heron
