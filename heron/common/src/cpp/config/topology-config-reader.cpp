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

#include "config/topology-config-reader.h"
#include <set>
#include "basics/basics.h"
#include "config/yaml-file-reader.h"
#include "config/topology-config-vars.h"
#include "errors/errors.h"
#include "network/network.h"
#include "proto/messages.h"
#include "threads/threads.h"
#include "yaml-cpp/yaml.h"


namespace heron {
namespace config {

TopologyConfigReader::TopologyConfigReader(EventLoop* eventLoop, const sp_string& _defaults_file)
    : YamlFileReader(eventLoop, _defaults_file) {
  LoadConfig();
}

TopologyConfigReader::~TopologyConfigReader() {}

void TopologyConfigReader::OnConfigFileLoad() {
  AddIfMissing(TopologyConfigVars::TOPOLOGY_DEBUG, "false");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_STMGRS, "1");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS, "30");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM, "1");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_MAX_SPOUT_PENDING, "100");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_ENABLE_ACKING, "false");
  AddIfMissing(TopologyConfigVars::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, "true");
}
}  // namespace config
}  // namespace heron
