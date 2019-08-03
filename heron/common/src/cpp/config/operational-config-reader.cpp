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
#include "config/operational-config-vars.h"
#include "config/operational-config-reader.h"

namespace heron {
namespace config {

OperationalConfigReader::OperationalConfigReader(std::shared_ptr<EventLoop> eventLoop,
                                                 const sp_string& _defaults_file)
    : YamlFileReader(eventLoop, _defaults_file) {
  LoadConfig();
}

OperationalConfigReader::~OperationalConfigReader() {}

sp_string OperationalConfigReader::GetTopologyReleaseOverride(const sp_string& _top_name) {
  if (config_[OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES]) {
    YAML::Node n = config_[OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES];
    if (n.IsMap() && n[_top_name]) {
      return n[_top_name].as<sp_string>();
    } else {
      return "";
    }
  } else {
    return "";
  }
}

void OperationalConfigReader::OnConfigFileLoad() {
  // Nothing really
}
}  // namespace config
}  // namespace heron
