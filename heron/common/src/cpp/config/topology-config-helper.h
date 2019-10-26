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

////////////////////////////////////////////////////////////////
//
// topology-config-helper.h
//
// This file defines some helper methods to get some
// variables.
//
///////////////////////////////////////////////////////////////
#ifndef TOPOLOGY_CONFIG_HELPERS_H_
#define TOPOLOGY_CONFIG_HELPERS_H_

#include <map>
#include <unordered_set>
#include <string>
#include <utility>
#include "basics/basics.h"
#include "proto/messages.h"
#include "config/topology-config-vars.h"

namespace heron {
namespace config {

class TopologyConfigHelper {
 public:
  static TopologyConfigVars::TopologyReliabilityMode
          GetReliabilityMode(const proto::api::Topology& _topology);

  // Are message timeouts enabled for this topology
  static bool EnableMessageTimeouts(const proto::api::Topology& _topology);

  // This returns the value of TOPOLOGY_STMGRS from the config
  static sp_int32 GetNumStMgrs(const proto::api::Topology& _topology);

  // The number of workers needed for this component's config
  // Essentially plucks the value of the TOPOLOGY_COMPONENT_PARALLELISM
  static sp_int32 GetComponentParallelism(const proto::api::Config& _config);
  static sp_int32 GetComponentParallelism(const proto::api::Topology& _topology,
                                          const std::string& _component);

  // The total number of workers needed accross all components for
  // this topology
  static sp_int32 GetTotalParallelism(const proto::api::Topology& _topology);

  static void GetComponentParallelismMap(const proto::api::Topology& _topology,
                                         std::map<std::string, sp_int32>& pmap);

  // This writes TOPOLOGY_COMPONENT_PARALLELISM to the value specified
  static void SetComponentParallelism(proto::api::Config* _config, sp_int32 _parallelism);

  // Gets the topology specific JVM childopts if any
  static std::string GetWorkerChildOpts(const proto::api::Topology& _topology);

  // Gets the TOPOLOGY_RELEASE_OVERRIDES for this topology if any
  // Returns empty string otherwise
  static std::string GetTopologyReleaseOverrides(const proto::api::Topology& _topology);

  // Does some sanity checking on the topology structure.
  // returns true if the structure is sane. False otherwise
  static bool IsTopologySane(const proto::api::Topology& _topology);

  // Strips the bolt/spout objects from the topology
  // and returns a new topology structure.
  static proto::api::Topology* StripComponentObjects(const proto::api::Topology& _topology);

  // Gets the per container CPU requested by this topology
  static sp_double64 GetContainerCpuRequested(const proto::api::Topology& _topology);

  // Gets the per container RAM requested by this topology
  static sp_int64 GetContainerRamRequested(const proto::api::Topology& _topology);

  // Get all the streams emitted by a component
  static void GetComponentStreams(const proto::api::Topology& _topology,
                                  const std::string& component,
                                  std::unordered_set<std::string>& retval);

  // Get the schema of the stream of a component
  static proto::api::StreamSchema* GetStreamSchema(proto::api::Topology& _topology,
                                                   const std::string& _component,
                                                   const std::string& _stream);

  // Get the sources for this component. The return value is a map of <componentName, streamId> to
  // Grouping map
  static void GetComponentSources(const proto::api::Topology& _topology,
                                  const std::string& _component,
                                  std::map<std::pair<std::string, std::string>,
                                           proto::api::Grouping>& retval);

  // Get the targets for this component. The return value is a map of <componentName, streamId> to
  // Grouping map
  static void GetComponentTargets(const proto::api::Topology& _topology,
                                  const std::string& _component,
                                  std::map<std::string,
                                           std::map<std::string, proto::api::Grouping>>& retval);

  // Get all the component names
  static void GetAllComponentNames(const proto::api::Topology& _topology,
                                   std::unordered_set<std::string>& retval);

  // Is this a spout
  static bool IsComponentSpout(const proto::api::Topology& _topology,
                               const std::string& _component);

  // Log this topology
  static void LogTopology(const proto::api::Topology& _topology);

  // Log this config
  static void LogConfig(const proto::api::Config& _config);

  // Should this stateful topology start from clean state
  static bool StatefulTopologyStartClean(const proto::api::Topology& _topology);

  // Gets the checkpoint interval for stateful topologies
  static sp_int64 GetStatefulCheckpointIntervalSecsWithDefault(
                  const proto::api::Topology& _topology, sp_int64 _default);

  // Gets the list of all spout component names
  static void GetSpoutComponentNames(const proto::api::Topology& _topology,
                                     std::unordered_set<std::string>& spouts);

  // Do we want to drop tuples upon backpressure detection
  static bool DropTuplesUponBackpressure(const proto::api::Topology& _topology);

  // Get runtime config key
  static std::string GetRuntimeConfigKey(const std::string& key);

  // Convert configs in map to runtime configs (append runtime postfix)
  static void ConvertToRuntimeConfigs(const std::map<std::string, std::string>& _origin,
                                      std::map<std::string, std::string>& _retval);

  // Return topology level config
  static void GetTopologyRuntimeConfig(const proto::api::Topology& _topology,
                                       std::map<std::string, std::string>& retval);

  // Update topology level config
  static void SetTopologyRuntimeConfig(proto::api::Topology* _topology,
                                       const std::map<std::string, std::string>& retval);


  // Return component level config
  static void GetComponentRuntimeConfig(const proto::api::Topology& _topology,
                                        const std::string& _component_name,
                                        std::map<std::string, std::string>& config);

  // Update component level config
  static void SetComponentRuntimeConfig(proto::api::Topology* _topology,
                                        const std::string& _component_name,
                                        const std::map<std::string, std::string>& config);

  // Get the topology config value given the config key
  static const std::string GetTopologyConfigValue(const proto::api::Topology& _topology,
                                                  const std::string& _key,
                                                  const std::string& _default);

  // Get the config value given component name and config key
  static const std::string GetComponentConfigValue(const proto::api::Topology& _topology,
                                                   const std::string& _component,
                                                   const std::string& _key,
                                                   const std::string& _default);

  // Get the value for a key in a config
  static const std::string GetConfigValue(const proto::api::Config& _config,
                                          const std::string& _key,
                                          const std::string& _default);

  // The output BPS of this component. Return -1 if not found
  static sp_int64 GetComponentOutputBPS(const proto::api::Topology& _topology,
                                        const std::string& _component);

  // Get reserved topology config key.
  static const char* GetReservedTopologyConfigKey();

 private:
  static bool GetBooleanConfigValue(const proto::api::Topology& _topology,
                                    const std::string& _config_name,
                                    bool _default_value);
  // Convert topology config to a key value map
  static void ConvertRuntimeConfigToKVMap(const proto::api::Config& _config,
                                          std::map<std::string, std::string>& retval);
  // Update topology config from a key value map
  static void UpdateRuntimeConfigFromKVMap(proto::api::Config* _config,
                                           const std::map<std::string, std::string>& _kv_map);
};
}  // namespace config
}  // namespace heron

#endif
