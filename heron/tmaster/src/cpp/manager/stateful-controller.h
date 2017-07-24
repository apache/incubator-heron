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

#ifndef __TMASTER_STATEFUL_HELPER_H_
#define __TMASTER_STATEFUL_HELPER_H_

#include <set>
#include <string>
#include "network/network.h"
#include "manager/tmaster.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class MetricsMgrSt;
class MultiCountMetric;
}
}  // namespace heron

namespace heron {
namespace tmaster {

class StatefulRestorer;
class StatefulCheckpointer;

// For Heron topologies running in exactly once semantics, the tmaster
// utilizes the stateful controller to handle all the work related with
// checkpointing and restoring from checkpoints. The statful controller
// offers methods to start checkpoint/restore. It also manages the state
// to track if the checkpoint/restore is done/still in progress.
//
// For more information please refer to the stateful processing design doc at
// https://docs.google.com/document/d/1pNuE77diSrYHb7vHPuPO3DZqYdcxrhywH_f7loVryCI/edit#
// and in particular the recovery section.
class StatefulController {
 public:
  explicit StatefulController(const std::string& _topology_name,
               proto::ckptmgr::StatefulConsistentCheckpoints* _ckpt,
               heron::common::HeronStateMgr* _state_mgr,
               std::chrono::high_resolution_clock::time_point _tmaster_start_time,
               common::MetricsMgrSt* _metrics_manager_client,
               std::function<void(std::string)> _ckpt_save_watcher);
  virtual ~StatefulController();
  // Start a new restore process
  void StartRestore(const StMgrMap& _stmgrs, bool _ignore_prev_checkpoints);
  // Called by tmaster when a Stmgr responds back with a RestoreTopologyStateResponse
  void HandleStMgrRestored(const std::string& _stmgr_id,
                           const std::string& _checkpoint_id,
                           int64_t _restore_txid,
                           proto::system::StatusCode _status,
                           const StMgrMap& _stmgrs);
  // Called when a new physical plan is made
  void RegisterNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  // Called its time to start a new checkpoint
  void StartCheckpoint(const StMgrMap& _stmgrs);

  // Called when we receive a InstanceStateStored message
  void HandleInstanceStateStored(const std::string& _checkpoint_id,
                                 const std::string& _packing_plan_id,
                                 const proto::system::Instance& _instance);

  // Check if response received from given stmgr during restore
  bool GotRestoreResponse(const std::string& _stmgr) const;

  bool RestoreInProgress() const;

 private:
  // Get the youngest ckpt id that is older than the given ckpt_id
  const std::string& GetNextInLineCheckpointId(const std::string& _ckpt_id);
  // Creates a new ckpt record adding the latest one
  proto::ckptmgr::StatefulConsistentCheckpoints*
    AddNewConsistentCheckpoint(const std::string& _new_checkpoint,
                               const std::string& _packing_plan);
  // Handler when statemgr saves the new checkpoint record
  void HandleCheckpointSave(proto::ckptmgr::StatefulConsistentCheckpoints* _new_ckpt,
                            proto::system::StatusCode _status);

  std::string topology_name_;
  proto::ckptmgr::StatefulConsistentCheckpoints* ckpt_record_;
  heron::common::HeronStateMgr* state_mgr_;
  StatefulCheckpointer* checkpointer_;
  StatefulRestorer* restorer_;
  common::MetricsMgrSt* metrics_manager_client_;
  common::MultiCountMetric* count_metrics_;
  std::function<void(std::string)> ckpt_save_watcher_;
};
}  // namespace tmaster
}  // namespace heron

#endif
