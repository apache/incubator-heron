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

#include "manager/stateful-controller.h"
#include <iostream>
#include <sstream>
#include <chrono>
#include <string>
#include <vector>
#include "manager/stateful-checkpointer.h"
#include "manager/stateful-restorer.h"
#include "manager/tmanager.h"
#include "manager/stmgrstate.h"
#include "metrics/metrics.h"
#include "basics/basics.h"
#include "errors/errors.h"

namespace heron {
namespace tmanager {

using std::make_shared;

const sp_string METRIC_RESTORE_START = "__restore_start";
const sp_string METRIC_RESTORE_STMGR_RESPONSE = "__restore_stmgr_response";
const sp_string METRIC_RESTORE_STMGR_RESPONSE_IGNORED = "__restore_stmgr_response_ignored";
const sp_string METRIC_RESTORE_STMGR_RESPONSE_NOTOK = "__restore_stmgr_response_notok";
const sp_string METRIC_CKPTMARKER_REQUESTS_SENT = "__ckptmarker_requests_sent";
const sp_string METRIC_CKPTMARKER_REQUESTS_NOTSENT = "__ckptmarker_requests_notsent";
const sp_string METRIC_INSTANCE_CKPT_SAVED = "__instance_ckpt_saved";
const sp_string METRIC_INSTANCE_CKPT_SAVED_IGNORED = "__instance_ckpt_saved_ignored";
const sp_string METRIC_GLOBAL_CONSISTENT_CKPT = "__globally_consistent_ckpt";


// TODO(nlu): make this number from config
const int32_t MOST_CHECKPOINTS_NUMBER = 5;

StatefulController::StatefulController(const std::string& _topology_name,
               shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt,
               shared_ptr<heron::common::HeronStateMgr> _state_mgr,
               std::chrono::high_resolution_clock::time_point _tmanager_start_time,
               shared_ptr<common::MetricsMgrSt> _metrics_manager_client,
               std::function<void(const proto::ckptmgr::StatefulConsistentCheckpoints&)>
                   _ckpt_save_watcher)
  : topology_name_(_topology_name),
    ckpt_record_(std::move(_ckpt)),
    state_mgr_(_state_mgr),
    metrics_manager_client_(_metrics_manager_client),
    ckpt_save_watcher_(_ckpt_save_watcher) {
  checkpointer_ = make_unique<StatefulCheckpointer>(_tmanager_start_time);
  restorer_ = make_unique<StatefulRestorer>();
  count_metrics_ = make_shared<common::MultiCountMetric>();

  metrics_manager_client_->register_metric("__stateful_controller", count_metrics_);
}

StatefulController::~StatefulController() {
  metrics_manager_client_->unregister_metric("__stateful_controller");
}

void StatefulController::StartRestore(const StMgrMap& _stmgrs, bool _ignore_prev_state) {
  count_metrics_->scope(METRIC_RESTORE_START)->incr();
  // TODO(sanjeev): Do we really need to start from most_recent_checkpoint?
  if (_ignore_prev_state) {
    restorer_->StartRestore("", _stmgrs);
  } else {
    restorer_->StartRestore(ckpt_record_->consistent_checkpoints(0).checkpoint_id(), _stmgrs);
  }
}

void StatefulController::HandleStMgrRestored(const std::string& _stmgr_id,
                                         const std::string& _checkpoint_id,
                                         int64_t _restore_txid,
                                         proto::system::StatusCode _status,
                                         const StMgrMap& _stmgrs) {
  count_metrics_->scope(METRIC_RESTORE_STMGR_RESPONSE)->incr();
  if (!restorer_->IsInProgress()) {
    LOG(WARNING) << "Got a Restored Topology State from stmgr "
                 << _stmgr_id << " for checkpoint " << _checkpoint_id
                 << " with txid " << _restore_txid << " when "
                 << " we are not in restore";
    count_metrics_->scope(METRIC_RESTORE_STMGR_RESPONSE_IGNORED)->incr();
    return;
  } else if (restorer_->GetRestoreTxid() != _restore_txid ||
             restorer_->GetCheckpointIdInProgress() != _checkpoint_id) {
    LOG(WARNING) << "Got a Restored Topology State from stmgr "
                 << _stmgr_id << " for checkpoint " << _checkpoint_id
                 << " with txid " << _restore_txid << " when "
                 << " we are in progress with checkpoint "
                 << restorer_->GetCheckpointIdInProgress() << " and txid "
                 << restorer_->GetRestoreTxid();
    count_metrics_->scope(METRIC_RESTORE_STMGR_RESPONSE_IGNORED)->incr();
    return;
  } else if (_status != proto::system::OK) {
    LOG(INFO) << "Got a Cannot Restore Topology State from stmgr "
              << _stmgr_id << " for checkpoint " << _checkpoint_id
              << " with txid " << _restore_txid << " because of "
              << _status;
    const std::string& new_ckpt_id = GetNextInLineCheckpointId(_checkpoint_id);
    if (new_ckpt_id.empty()) {
      LOG(INFO) << "Next viable checkpoint id is empty";
    }
    count_metrics_->scope(METRIC_RESTORE_STMGR_RESPONSE_NOTOK)->incr();
    restorer_->StartRestore(new_ckpt_id, _stmgrs);
  } else {
    LOG(INFO) << "Got a Restored Topology State from stmgr "
              << _stmgr_id << " for checkpoint " << _checkpoint_id
              << " with txid " << _restore_txid;
    restorer_->HandleStMgrRestored(_stmgr_id, _checkpoint_id, _restore_txid, _stmgrs);
  }
}

void StatefulController::RegisterNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  checkpointer_->RegisterNewPhysicalPlan(_pplan);
}

void StatefulController::StartCheckpoint(const StMgrMap& _stmgrs) {
  if (restorer_->IsInProgress()) {
    LOG(INFO) << "Will not send checkpoint messages to stmgr because "
              << "we are in restore";
    count_metrics_->scope(METRIC_CKPTMARKER_REQUESTS_NOTSENT)->incr();
    return;
  }
  count_metrics_->scope(METRIC_CKPTMARKER_REQUESTS_SENT)->incr();
  checkpointer_->StartCheckpoint(_stmgrs);
}

void StatefulController::HandleInstanceStateStored(const std::string& _checkpoint_id,
                                                   const std::string& _packing_plan_id,
                                                   const proto::system::Instance& _instance) {
  count_metrics_->scope(METRIC_INSTANCE_CKPT_SAVED)->incr();

  if (restorer_->IsInProgress()) {
    LOG(INFO) << "Ignoring the Instance State because we are in Restore";
    count_metrics_->scope(METRIC_INSTANCE_CKPT_SAVED_IGNORED)->incr();
    return;
  }

  if (checkpointer_->HandleInstanceStateStored(_checkpoint_id, _instance)) {
    // This is now a globally consistent checkpoint
    count_metrics_->scope(METRIC_GLOBAL_CONSISTENT_CKPT)->incr();
    auto new_ckpt_record = AddNewConsistentCheckpoint(_checkpoint_id, _packing_plan_id);

    state_mgr_->SetStatefulCheckpoints(topology_name_, new_ckpt_record,
            std::bind(&StatefulController::HandleCheckpointSave, this, new_ckpt_record,
                    std::placeholders::_1));
  }
}

void StatefulController::HandleCheckpointSave(
        shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _new_ckpt,
        proto::system::StatusCode _status) {
  if (_status == proto::system::OK) {
    ckpt_record_ = std::move(_new_ckpt);

    LOG(INFO) << "Successfully saved "
              << ckpt_record_->consistent_checkpoints(0).checkpoint_id()
              << " as the latest globally consistent checkpoint";

    ckpt_save_watcher_(*ckpt_record_);
  } else {
    LOG(ERROR) << "Error saving " << _new_ckpt->consistent_checkpoints(0).checkpoint_id()
              << " as the new globally consistent checkpoint "
              << _status;
  }
}

const std::string& StatefulController::GetNextInLineCheckpointId(const std::string& _ckpt_id) {
  if (_ckpt_id.empty()) {
    // There cannot be any checkpoints that are older than empty checkpoint
    LOG(FATAL) << "Could not recover even from the empty state";
  }
  for (int32_t i = 0; i < ckpt_record_->consistent_checkpoints_size(); ++i) {
    if (ckpt_record_->consistent_checkpoints(i).checkpoint_id() == _ckpt_id) {
      if (i < ckpt_record_->consistent_checkpoints_size() - 1) {
        return ckpt_record_->consistent_checkpoints(i + 1).checkpoint_id();
      } else {
        return EMPTY_STRING;
      }
    }
  }
  return EMPTY_STRING;
}

shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints>
StatefulController::AddNewConsistentCheckpoint(const std::string& _new_checkpoint,
                                           const std::string& _packing_plan) {
  auto new_record = make_shared<proto::ckptmgr::StatefulConsistentCheckpoints>();
  auto new_consistent_checkpoint = new_record->add_consistent_checkpoints();
  new_consistent_checkpoint->set_checkpoint_id(_new_checkpoint);
  new_consistent_checkpoint->set_packing_plan_id(_packing_plan);
  for (int32_t i = 0; i < ckpt_record_->consistent_checkpoints_size() &&
                      new_record->consistent_checkpoints_size() < MOST_CHECKPOINTS_NUMBER; ++i) {
    new_consistent_checkpoint = new_record->add_consistent_checkpoints();
    new_consistent_checkpoint->set_checkpoint_id(
      ckpt_record_->consistent_checkpoints(i).checkpoint_id());
    new_consistent_checkpoint->set_packing_plan_id(
      ckpt_record_->consistent_checkpoints(i).packing_plan_id());
  }

  return new_record;
}

bool StatefulController::GotRestoreResponse(const std::string& _stmgr) const {
  CHECK(restorer_->IsInProgress());
  return restorer_->GotResponse(_stmgr);
}

bool StatefulController::RestoreInProgress() const {
  return restorer_->IsInProgress();
}
}  // namespace tmanager
}  // namespace heron
