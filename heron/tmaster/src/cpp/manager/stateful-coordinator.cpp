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

#include "tmaster/src/cpp/manager/stateful-coordinator.h"
#include <iostream>
#include <sstream>
#include <chrono>
#include <vector>
#include "config/topology-config-helper.h"
#include "manager/tmaster.h"
#include "manager/stmgrstate.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "proto/tmaster.pb.h"

namespace heron {
namespace tmaster {

StatefulCoordinator::StatefulCoordinator(
  std::chrono::high_resolution_clock::time_point _tmaster_start_time,
  proto::api::Topology* _topology)
  : tmaster_start_time_(_tmaster_start_time),
    topology_(_topology) {
  // nothing really
}

StatefulCoordinator::~StatefulCoordinator() { }

sp_string StatefulCoordinator::GenerateCheckpointId() {
  // TODO(skukarni) Should we append any topology name/id stuff?
  std::ostringstream tag;
  tag << tmaster_start_time_.time_since_epoch().count()
      << "-" << time(NULL);
  return tag.str();
}

void StatefulCoordinator::DoCheckpoint(const StMgrMap& _stmgrs) {
  // Generate the checkpoint id
  sp_string checkpoint_id = GenerateCheckpointId();

  // Send the checkpoint message to all active stmgrs
  LOG(INFO) << "Sending checkpoint tag " << checkpoint_id
            << " to all strmgrs";
  std::vector<sp_string> spouts = config::TopologyConfigHelper::GetSpoutComponentNames(*topology_);
  for (size_t i = 0; i < spouts.size(); ++i) {
    StMgrMapConstIter iter;
    for (iter = _stmgrs.begin(); iter != _stmgrs.end(); ++iter) {
      proto::ckptmgr::StatefulCheckpoint request;
      request.set_checkpoint_id(checkpoint_id);
      request.set_component_name(spouts[i]);
      iter->second->StatefulNewCheckpoint(request);
    }
  }
}

}  // namespace tmaster
}  // namespace heron
