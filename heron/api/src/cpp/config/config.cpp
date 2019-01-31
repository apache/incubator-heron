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

#include <set>
#include <string>

#include "config/config.h"

namespace heron {
namespace api {
namespace config {

// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_DEBUG = "topology.debug";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_STMGRS = "topology.stmgrs";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_RELIABILITY_MODE = "topology.reliability.mode";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_MAX_CPU_HINT = "topology.container.max.cpu.hint";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_MAX_RAM_HINT = "topology.container.max.ram.hint";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_MAX_DISK_HINT = "topology.container.max.disk.hint";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_PADDING_PERCENTAGE =
                                                    "topology.container.padding.percentage";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_CONTAINER_RAM_PADDING = "topology.container.ram.padding";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_COMPONENT_CPUMAP = "topology.component.cpumap";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_COMPONENT_DISKMAP = "topology.component.diskmap";

// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname";
// NOLINTNEXTLINE
const std::string Config::TOPOLOGY_NAME = "topology.name";

const std::set<std::string> Config::apiVars_ = {
  Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS,
  Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,
  Config::TOPOLOGY_DEBUG,
  Config::TOPOLOGY_STMGRS,
  Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
  Config::TOPOLOGY_COMPONENT_PARALLELISM,
  Config::TOPOLOGY_MAX_SPOUT_PENDING,
  Config::TOPOLOGY_RELIABILITY_MODE,
  Config::TOPOLOGY_CONTAINER_CPU_REQUESTED,
  Config::TOPOLOGY_CONTAINER_RAM_REQUESTED,
  Config::TOPOLOGY_CONTAINER_DISK_REQUESTED,
  Config::TOPOLOGY_CONTAINER_MAX_CPU_HINT,
  Config::TOPOLOGY_CONTAINER_MAX_RAM_HINT,
  Config::TOPOLOGY_CONTAINER_MAX_DISK_HINT,
  Config::TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
  Config::TOPOLOGY_CONTAINER_RAM_PADDING,
  Config::TOPOLOGY_COMPONENT_CPUMAP,
  Config::TOPOLOGY_COMPONENT_RAMMAP,
  Config::TOPOLOGY_COMPONENT_DISKMAP,
  Config::TOPOLOGY_NAME
};

void Config::setTopologyReliabilityMode(Config::TopologyReliabilityMode mode) {
  switch (mode) {
    case Config::TopologyReliabilityMode::ATMOST_ONCE:
      config_[Config::TOPOLOGY_RELIABILITY_MODE] = "ATMOST_ONCE";
      break;
    case Config::TopologyReliabilityMode::ATLEAST_ONCE:
      config_[Config::TOPOLOGY_RELIABILITY_MODE] = "ATLEAST_ONCE";
      break;
    case Config::TopologyReliabilityMode::EFFECTIVELY_ONCE:
      config_[Config::TOPOLOGY_RELIABILITY_MODE] = "EFFECTIVELY_ONCE";
      break;
    default:
      config_[Config::TOPOLOGY_RELIABILITY_MODE] = "ATMOST_ONCE";
      break;
  }
}

}  // namespace config
}  // namespace api
}  // namespace heron
