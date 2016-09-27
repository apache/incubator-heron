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
#include <sstream>
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

bool TopologyConfigHelper::IsAckingEnabled(const proto::api::Topology& _topology) {
  sp_string value_true_ = "true";
  std::set<sp_string> topology_config;
  if (_topology.has_topology_config()) {
    const proto::api::Config& cfg = _topology.topology_config();
    for (sp_int32 i = 0; i < cfg.kvs_size(); ++i) {
      if (cfg.kvs(i).key() == TopologyConfigVars::TOPOLOGY_ENABLE_ACKING) {
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
  std::ostringstream ostr;
  ostr << _parallelism;
  kv->set_value(ostr.str());
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
}  // namespace config
}  // namespace heron
