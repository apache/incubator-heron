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

#include "manager/stateful-restorer.h"
#include <string>
#include "manager/stmgrstate.h"

namespace heron {
namespace tmanager {

StatefulRestorer::StatefulRestorer()
  : in_progress_(false),
    restore_txid_(0) {
}

StatefulRestorer::~StatefulRestorer() { }

void StatefulRestorer::StartRestore(const std::string& _checkpoint_id, const StMgrMap& _stmgrs) {
  if (in_progress_) {
    LOG(WARNING) << "Starting a restore for checkpoint "
                 << _checkpoint_id << " when we are already busy"
                 << " within restore of " << checkpoint_id_in_progress_
                 << ". The restore result of " << checkpoint_id_in_progress_
                 << " will be abandoned";
  }
  in_progress_ = true;
  checkpoint_id_in_progress_ = _checkpoint_id;
  unreplied_stmgrs_.clear();
  ++restore_txid_;
  LOG(INFO) << "Starting a 2 phase commit Restore for checkpoint "
            << _checkpoint_id << " and restore txid "
            << restore_txid_;
  proto::ckptmgr::RestoreTopologyStateRequest request;
  request.set_checkpoint_id(_checkpoint_id);
  request.set_restore_txid(restore_txid_);
  for (auto kv : _stmgrs) {
    kv.second->SendRestoreTopologyStateMessage(request);
    unreplied_stmgrs_.insert(kv.first);
  }
}

bool StatefulRestorer::GotResponse(const std::string& _stmgr) const {
  return unreplied_stmgrs_.find(_stmgr) == unreplied_stmgrs_.end();
}

void StatefulRestorer::HandleStMgrRestored(const std::string& _stmgr_id,
                                           const std::string& _checkpoint_id,
                                           int64_t _restore_txid,
                                           const StMgrMap& _stmgrs) {
  CHECK(in_progress_);
  CHECK(_checkpoint_id == checkpoint_id_in_progress_);
  CHECK(_restore_txid == restore_txid_);
  unreplied_stmgrs_.erase(_stmgr_id);
  if (unreplied_stmgrs_.empty()) {
    Finish2PhaseCommit(_stmgrs);
  }
}

void StatefulRestorer::Finish2PhaseCommit(const StMgrMap& _stmgrs) {
  LOG(INFO) << "Finishing Stateful 2 Phase Commit since all stmgrs have replied back";
  CHECK(unreplied_stmgrs_.empty());
  for (auto kv : _stmgrs) {
    kv.second->SendStartStatefulProcessingMessage(checkpoint_id_in_progress_);
  }
  in_progress_ = false;
  checkpoint_id_in_progress_ = "";
}

}  // namespace tmanager
}  // namespace heron
