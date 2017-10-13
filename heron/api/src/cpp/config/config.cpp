/*
 * Copyright 2017 Twitter, Inc.
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

#include <set>
#include <string>

#include "config/config.h"

namespace heron {
namespace api {
namespace config {

const std::string Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS = "topology.tick.tuple.freq.secs";
const std::string Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS = "topology.enable.message.timeouts";
const std::string Config::TOPOLOGY_DEBUG = "topology.debug";
const std::string Config::TOPOLOGY_STMGRS = "topology.stmgrs";
const std::string Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS = "topology.message.timeout.secs";
const std::string Config::TOPOLOGY_COMPONENT_PARALLELISM = "topology.component.parallelism";
const std::string Config::TOPOLOGY_MAX_SPOUT_PENDING = "topology.max.spout.pending";
const std::string Config::TOPOLOGY_ENABLE_ACKING = "topology.acking";
const std::string Config::TOPOLOGY_CONTAINER_CPU_REQUESTED = "topology.container.cpu";
const std::string Config::TOPOLOGY_CONTAINER_RAM_REQUESTED = "topology.container.ram";
const std::string Config::TOPOLOGY_CONTAINER_DISK_REQUESTED = "topology.container.disk";
const std::string Config::TOPOLOGY_CONTAINER_MAX_CPU_HINT = "topology.container.max.cpu.hint";
const std::string Config::TOPOLOGY_CONTAINER_MAX_RAM_HINT = "topology.container.max.ram.hint";
const std::string Config::TOPOLOGY_CONTAINER_MAX_DISK_HINT = "topology.container.max.disk.hint";
const std::string Config::TOPOLOGY_CONTAINER_PADDING_PERCENTAGE =
                                                    "topology.container.padding.percentage";
const std::string Config::TOPOLOGY_COMPONENT_RAMMAP = "topology.component.rammap";
const std::string Config::TOPOLOGY_SERIALIZER_CLASSNAME = "topology.serializer.classname";
const std::string Config::TOPOLOGY_NAME = "topology.name";

const std::set<std::string> Config::apiVars_ = {
  Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS,
  Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS,
  Config::TOPOLOGY_DEBUG,
  Config::TOPOLOGY_STMGRS,
  Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
  Config::TOPOLOGY_COMPONENT_PARALLELISM,
  Config::TOPOLOGY_MAX_SPOUT_PENDING,
  Config::TOPOLOGY_ENABLE_ACKING,
  Config::TOPOLOGY_CONTAINER_CPU_REQUESTED,
  Config::TOPOLOGY_CONTAINER_RAM_REQUESTED,
  Config::TOPOLOGY_CONTAINER_DISK_REQUESTED,
  Config::TOPOLOGY_CONTAINER_MAX_CPU_HINT,
  Config::TOPOLOGY_CONTAINER_MAX_RAM_HINT,
  Config::TOPOLOGY_CONTAINER_MAX_DISK_HINT,
  Config::TOPOLOGY_CONTAINER_PADDING_PERCENTAGE,
  Config::TOPOLOGY_COMPONENT_RAMMAP,
  Config::TOPOLOGY_NAME
};

}  // namespace config
}  // namespace api
}  // namespace heron
