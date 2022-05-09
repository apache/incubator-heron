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

#ifndef __TMANAGER_STATEFUL_CHECKPOINTER_H_
#define __TMANAGER_STATEFUL_CHECKPOINTER_H_

#include <string>
#include <unordered_set>
#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "manager/tmanager.h"
#include "basics/basics.h"


namespace heron {
namespace proto {
class PhysicalPlan;
}
}

namespace heron {
namespace tmanager {

/**
 * A StatefulCheckpointer is responsible for sending NewStatefulCheckpoint
 * requests to stmgrs to start checkpoints and keep tracking of the ongoing
 * checkpointing progress.
 *
 */
class StatefulCheckpointer {
 public:
  explicit StatefulCheckpointer(std::chrono::high_resolution_clock::time_point _tmanager_start_time);
  virtual ~StatefulCheckpointer();
  void RegisterNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  void StartCheckpoint(const StMgrMap& _stmgrs);

  // Called by tmanager when a InstanceStateStored message is received
  // Return true if this completes a globally consistent checkpoint
  // for this _checkpoint_id
  bool HandleInstanceStateStored(const std::string& _checkpoint_id,
                                 const proto::system::Instance& _instance);

 private:
  sp_string GenerateCheckpointId();

  std::chrono::high_resolution_clock::time_point tmanager_start_time_;

  // Current partially consistent checkpoint
  // for which still some more states need to be saved
  std::string current_partial_checkpoint_;

  // The set of all tasks
  std::unordered_set<sp_int32> all_tasks_;

  // The set of tasks from which we still need confirmation
  // of state storage before we can declare it a globally
  // consistent checkpoint
  std::unordered_set<sp_int32> partial_checkpoint_remaining_tasks_;
};
}  // namespace tmanager
}  // namespace heron

#endif
