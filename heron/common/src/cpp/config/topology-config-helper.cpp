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

#include "config/topology-config-helper.h"
#include <map>
#include <set>
#include <string>
#include "basics/basics.h"
#include "config/operational-config-vars.h"
#include "config/topology-config-vars.h"
#include "errors/errors.h"
#include "network/network.h"
#include "proto/messages.h"
#include "threads/threads.h"

namespace heron {
namespace config {

TopologyConfigVars::TopologyReliabilityMode StringToReliabilityMode(const std::string& _mode) {
  if (_mode == "ATMOST_ONCE") {
    return TopologyConfigVars::TopologyReliabilityMode::ATMOST_ONCE;
  } else if (_mode == "ATLEAST_ONCE") {
    return TopologyConfigVars::TopologyReliabilityMode::ATLEAST_ONCE;
  } else if (_mode == "EFFECTIVELY_ONCE") {
    return TopologyConfigVars::TopologyReliabilityMode::EFFECTIVELY_ONCE;
  } else {
    LOG(FATAL) << "Unknown Topology Reliability Mode " << _mode;
    return TopologyConfigVars::TopologyReliabilityMode::ATMOST_ONCE;
  }
}

TopologyConfigVars::TopologyReliabilityMode
TopologyConfigHelper::GetReliabilityMode(const proto::api::Topology& _topology) {
  sp_string value_true_ = "true";
  if (_topology.has_topology_config()) {
    const proto::api::Config& cfg = _topology.topology_config();
    // First search for reliabiliy mode
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_RELIABILITY_MODE) {
        return StringToReliabilityMode(cfg.kvs(i).value());
      }
    }
    // Nothing was found wrt reliability mode.
    // The following is strictly for backwards compat
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_ENABLE_ACKING) {
        if (value_true_.compare(cfg.kvs(i).value().c_str()) == 0) {
          return TopologyConfigVars::TopologyReliabilityMode::ATLEAST_ONCE;
        } else {
          return TopologyConfigVars::TopologyReliabilityMode::ATMOST_ONCE;
        }
      }
    }
  }

  return TopologyConfigVars::TopologyReliabilityMode::ATMOST_ONCE;
}

bool TopologyConfigHelper::EnableMessageTimeouts(const proto::api::Topology& _topology) {
  sp_string value_true_ = "true";
  std::set<sp_string> topology_config;
  if (_topology.has_topology_config()) {
    const proto::api::Config& cfg = _topology.topology_config();
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS) {
        return value_true_.compare(cfg.kvs(i).value().c_str()) == 0;
      }
    }
  }
  return false;
}

sp_int32 TopologyConfigHelper::GetNumStMgrs(const proto::api::Topology& _topology) {
  std::set<sp_string> topology_config;
  if (_topology.has_topology_config()) {
    const proto::api::Config& cfg = _topology.topology_config();
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_STMGRS) {
        return atoi(cfg.kvs(i).value().c_str());
      }
    }
  }

  LOG(ERROR) << "No TOPOLOGY_STMGRS defined" << std::endl;
  ::exit(1);
  return -1;  // keep compiler happy
}

sp_int32 TopologyConfigHelper::GetComponentParallelism(const proto::api::Config& _config) {
  for (sp_int32 i = 0; i < _config.kvs_size(); ++i) {
    if (_config.kvs(i).key() == TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM) {
      return atoi(_config.kvs(i).value().c_str());
    }
  }
  CHECK(false) << "Topology config - no component parallelism hints";
  return -1;  // keep compiler happy
}

sp_int32 TopologyConfigHelper::GetTotalParallelism(const proto::api::Topology& _topology) {
  sp_int32 nworkers = 0;
  for (int i = 0; i < _topology.spouts_size(); ++i) {
    const proto::api::Config& spout_config = _topology.spouts(i).comp().config();
    nworkers += GetComponentParallelism(spout_config);
  }
  for (int i = 0; i < _topology.bolts_size(); ++i) {
    const proto::api::Config& bolt_config = _topology.bolts(i).comp().config();
    nworkers += GetComponentParallelism(bolt_config);
  }
  return nworkers;
}

void TopologyConfigHelper::GetComponentParallelismMap(const proto::api::Topology& _topology,
                                                      std::map<std::string, sp_int32>& pmap) {
  for (int i = 0; i < _topology.spouts_size(); ++i) {
    const proto::api::Config& spout_config = _topology.spouts(i).comp().config();
    pmap[_topology.spouts(i).comp().name()] = GetComponentParallelism(spout_config);
  }
  for (int i = 0; i < _topology.bolts_size(); ++i) {
    const proto::api::Config& bolt_config = _topology.bolts(i).comp().config();
    pmap[_topology.bolts(i).comp().name()] = GetComponentParallelism(bolt_config);
  }
}

void TopologyConfigHelper::SetComponentParallelism(proto::api::Config* _config,
                                                   sp_int32 _parallelism) {
  proto::api::Config::KeyValue* kv = _config->add_kvs();
  kv->set_key(TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
  kv->set_value(std::to_string(_parallelism));
}

sp_string TopologyConfigHelper::GetWorkerChildOpts(const proto::api::Topology& _topology) {
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_WORKER_CHILDOPTS) {
      return cfg.kvs(i).value();
    }
  }
  return "";
}

sp_string TopologyConfigHelper::GetTopologyReleaseOverrides(const proto::api::Topology& _topology) {
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == OperationalConfigVars::TOPOLOGY_RELEASE_OVERRIDES) {
      return cfg.kvs(i).value();
    }
  }
  return "";
}

bool TopologyConfigHelper::IsTopologySane(const proto::api::Topology& _topology) {
  std::set<std::string> component_names;
  for (sp_int32 i = 0; i < _topology.spouts_size(); ++i) {
    if (component_names.find(_topology.spouts(i).comp().name()) != component_names.end()) {
      LOG(ERROR) << "Component names are not unique " << _topology.spouts(i).comp().name() << "\n";
      return false;
    }
    component_names.insert(_topology.spouts(i).comp().name());
  }

  for (sp_int32 i = 0; i < _topology.bolts_size(); ++i) {
    if (component_names.find(_topology.bolts(i).comp().name()) != component_names.end()) {
      LOG(ERROR) << "Component names are not unique " << _topology.bolts(i).comp().name() << "\n";
      return false;
    }
    component_names.insert(_topology.bolts(i).comp().name());
  }

  sp_int32 total_parallelism = GetTotalParallelism(_topology);
  if (GetNumStMgrs(_topology) > total_parallelism) {
    LOG(ERROR) << "Number of stmgrs are greater than "
               << "total parallelism"
               << "\n";
    return false;
  }

  return true;
}

proto::api::Topology* TopologyConfigHelper::StripComponentObjects(
    const proto::api::Topology& _topology) {
  proto::api::Topology* ret = new proto::api::Topology();
  ret->CopyFrom(_topology);
  for (sp_int32 i = 0; i < ret->spouts_size(); ++i) {
    if (ret->mutable_spouts(i)->mutable_comp()->has_serialized_object()) {
      ret->mutable_spouts(i)->mutable_comp()->clear_serialized_object();
    }
  }
  for (sp_int32 i = 0; i < ret->bolts_size(); ++i) {
    if (ret->mutable_bolts(i)->mutable_comp()->has_serialized_object()) {
      ret->mutable_bolts(i)->mutable_comp()->clear_serialized_object();
    }
  }
  return ret;
}

sp_double64 TopologyConfigHelper::GetContainerCpuRequested(const proto::api::Topology& _topology) {
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_CONTAINER_CPU_REQUESTED) {
      return atof(cfg.kvs(i).value().c_str());
    }
  }
  // Hmmm.. There was no value specified. The default is to allocate one cpu
  // per component on a stmgr
  sp_int32 total_parallelism = TopologyConfigHelper::GetTotalParallelism(_topology);
  sp_int32 nstmgrs = TopologyConfigHelper::GetNumStMgrs(_topology);
  return (total_parallelism / nstmgrs) + (total_parallelism % nstmgrs);
}

sp_int64 TopologyConfigHelper::GetContainerRamRequested(const proto::api::Topology& _topology) {
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_CONTAINER_RAM_REQUESTED) {
      return atol(cfg.kvs(i).value().c_str());
    }
  }
  // Hmmm.. There was no value specified. The default is to allocate 1G
  // per component on a stmgr
  sp_int32 total_parallelism = TopologyConfigHelper::GetTotalParallelism(_topology);
  sp_int32 nstmgrs = TopologyConfigHelper::GetNumStMgrs(_topology);
  sp_int64 max_components_per_container =
      (total_parallelism / nstmgrs) + (total_parallelism % nstmgrs);
  return max_components_per_container * 1073741824l;
}

void TopologyConfigHelper::GetComponentStreams(const proto::api::Topology& _topology,
                                               const std::string& _component,
                                               std::unordered_set<std::string>& retval) {
  for (auto spout : _topology.spouts()) {
    if (spout.comp().name() == _component) {
      for (auto output : spout.outputs()) {
        retval.insert(output.stream().id());
      }
    }
  }
  for (auto bolt : _topology.bolts()) {
    if (bolt.comp().name() == _component) {
      for (auto output : bolt.outputs()) {
        retval.insert(output.stream().id());
      }
    }
  }
}

proto::api::StreamSchema*
TopologyConfigHelper::GetStreamSchema(proto::api::Topology& _topology,
                                      const std::string& _component,
                                      const std::string& _stream) {
  for (int i = 0; i < _topology.spouts_size(); ++i) {
    auto spout = _topology.mutable_spouts(i);
    if (spout->comp().name() == _component) {
      for (int j = 0; j < spout->outputs_size(); ++j) {
        proto::api::OutputStream* output = spout->mutable_outputs(j);
        if (output->stream().id() == _stream) {
          return output->mutable_schema();
        }
      }
      return NULL;
    }
  }
  for (int i = 0; i < _topology.bolts_size(); ++i) {
    auto bolt = _topology.mutable_bolts(i);
    if (bolt->comp().name() == _component) {
      for (int j = 0; j < bolt->outputs_size(); ++j) {
        proto::api::OutputStream* output = bolt->mutable_outputs(j);
        if (output->stream().id() == _stream) {
          return output->mutable_schema();
        }
      }
      return NULL;
    }
  }
  return NULL;
}

void TopologyConfigHelper::GetComponentSources(const proto::api::Topology& _topology,
                                               const std::string& _component,
                                               std::map<std::pair<std::string, std::string>,
                                                        proto::api::Grouping>& retval) {
  // only bolts have sources
  for (auto bolt : _topology.bolts()) {
    if (bolt.comp().name() == _component) {
      for (auto ins : bolt.inputs()) {
        retval[std::make_pair(ins.stream().component_name(), ins.stream().id())] = ins.gtype();
      }
    }
  }
}

void TopologyConfigHelper::GetComponentTargets(const proto::api::Topology& _topology,
                                               const std::string& _component,
                        std::map<std::string,
                                 std::map<std::string, proto::api::Grouping>>& retval) {
  // only bolts have inputs
  for (auto bolt : _topology.bolts()) {
    for (auto ins : bolt.inputs()) {
      if (ins.stream().component_name() == _component) {
        if (retval.find(_component) == retval.end()) {
          retval[_component] = std::map<std::string, proto::api::Grouping>();
        }
        retval[_component][ins.stream().id()] = ins.gtype();
      }
    }
  }
}

void TopologyConfigHelper::GetAllComponentNames(const proto::api::Topology& _topology,
                                                std::unordered_set<std::string>& retval) {
  for (auto spout : _topology.spouts()) {
    retval.insert(spout.comp().name());
  }
  for (auto bolt : _topology.bolts()) {
    retval.insert(bolt.comp().name());
  }
}

bool TopologyConfigHelper::IsComponentSpout(const proto::api::Topology& _topology,
                                            const std::string& _component) {
  for (auto spout : _topology.spouts()) {
    if (spout.comp().name() == _component) return true;
  }
  return false;
}

void TopologyConfigHelper::LogTopology(const proto::api::Topology& _topology) {
  LOG(INFO) << "Printing Topology";
  LOG(INFO) << "Topology Name: " << _topology.name();
  LOG(INFO) << "Topology Id: " << _topology.id();
  LOG(INFO) << "Topology State: " << _topology.state();
  LOG(INFO) << "Topology Config:";
  LogConfig(_topology.topology_config());
  for (int i = 0; i < _topology.spouts_size(); ++i) {
    LOG(INFO) << "Spout Info: ";
    LOG(INFO) << "\tName: " << _topology.spouts(i).comp().name();
    LOG(INFO) << "\tSpec: " << _topology.spouts(i).comp().spec();
    LOG(INFO) << "\tConfig: ";
    LogConfig(_topology.spouts(i).comp().config());
  }
  for (int i = 0; i < _topology.bolts_size(); ++i) {
    LOG(INFO) << "Bolt Info: ";
    LOG(INFO) << "\tName: " << _topology.bolts(i).comp().name();
    LOG(INFO) << "\tSpec: " << _topology.bolts(i).comp().spec();
    LOG(INFO) << "\tConfig: ";
    LogConfig(_topology.bolts(i).comp().config());
  }
}

void TopologyConfigHelper::LogConfig(const proto::api::Config& _config) {
  for (int i = 0; i < _config.kvs_size(); ++i) {
    if (_config.kvs(i).type() == proto::api::ConfigValueType::STRING_VALUE) {
      LOG(INFO) << "\t" << _config.kvs(i).key() << ": " << _config.kvs(i).value();
    } else {
      LOG(INFO) << "\t" << _config.kvs(i).key();
    }
  }
}

bool TopologyConfigHelper::StatefulTopologyStartClean(const proto::api::Topology& _topology) {
  return GetBooleanConfigValue(_topology,
                               TopologyConfigVars::TOPOLOGY_STATEFUL_START_CLEAN, false);
}

sp_int64 TopologyConfigHelper::GetStatefulCheckpointIntervalSecsWithDefault(
                               const proto::api::Topology& _topology,
                               sp_int64 _default) {
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL_SECONDS) {
      return atol(cfg.kvs(i).value().c_str());
    }
  }
  // There was no value specified. Return the default
  return _default;
}

void TopologyConfigHelper::GetSpoutComponentNames(const proto::api::Topology& _topology,
                                                  std::unordered_set<std::string> spouts) {
  for (int i = 0; i < _topology.spouts_size(); ++i) {
    spouts.insert(_topology.spouts(i).comp().name());
  }
}

bool TopologyConfigHelper::GetBooleanConfigValue(const proto::api::Topology& _topology,
                                                 const std::string& _config_name,
                                                 bool _default_value) {
  sp_string value_true_ = "true";
  const proto::api::Config& cfg = _topology.topology_config();
  for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
    if (cfg.kvs(i).key() == _config_name) {
      return value_true_.compare(cfg.kvs(i).value().c_str()) == 0;
    }
  }
  return _default_value;
}
}  // namespace config
}  // namespace heron
