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
#include <functional>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>
#include "manager/instance-server.h"
#include "manager/ckptmgr-client.h"
#include "manager/stmgr-clientmgr.h"
#include "util/tuple-cache.h"
#include "metrics/metrics.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

using std::make_shared;

// Stats for restore
const sp_string METRIC_START_RESTORE = "__start_restore";
const sp_string METRIC_START_RESTORE_IN_PROGRESS = "__start_restore_in_progress";
const sp_string METRIC_START_RESTORE_FAILED = "__start_restore_failed";
const sp_string METRIC_CKPT_REQUESTS = "__ckpt_requests";
const sp_string METRIC_CKPT_RESPONSES = "__ckpt_responses";
const sp_string METRIC_CKPT_RESPONSES_IGNORED = "__ckpt_responses_ignored";
const sp_string METRIC_CKPT_RESPONSES_ERROR = "__ckpt_responses_error";
const sp_string METRIC_INSTANCE_RESTORE_REQUESTS = "__instance_restore_requests";
const sp_string METRIC_INSTANCE_RESTORE_RESPONSES = "__instance_restore_responses";
const sp_string METRIC_INSTANCE_RESTORE_RESPONSES_IGNORED = "__instance_restore_response_ignored";

StatefulRestorer::StatefulRestorer(shared_ptr<CkptMgrClient> _ckptmgr,
                             shared_ptr<StMgrClientMgr> _clientmgr,
                             shared_ptr<TupleCache> _tuple_cache,
                             shared_ptr<InstanceServer> _server,
                             shared_ptr<common::MetricsMgrSt> const& _metrics_manager_client,
                             std::function<void(proto::system::StatusCode,
                                                std::string, sp_int64)> _restore_done_watcher) {
  ckptmgr_ = _ckptmgr;
  clientmgr_ = _clientmgr;
  tuple_cache_ = _tuple_cache;
  server_ = _server;

  in_progress_ = false;
  restore_done_watcher_ = _restore_done_watcher;
  metrics_manager_client_ = _metrics_manager_client;
  multi_count_metric_ = make_shared<common::MultiCountMetric>();
  time_spent_metric_  = make_shared<common::TimeSpentMetric>();
  metrics_manager_client_->register_metric("__stateful_restore_count", multi_count_metric_);
  metrics_manager_client_->register_metric("__stateful_restore_time", time_spent_metric_);
}

StatefulRestorer::~StatefulRestorer() {
  metrics_manager_client_->unregister_metric("__stateful_restore_count");
  metrics_manager_client_->unregister_metric("__stateful_restore_time");
}

void StatefulRestorer::StartRestore(const std::string& _checkpoint_id, sp_int64 _restore_txid,
                                    const std::unordered_set<sp_int32>& _local_taskids,
                                    proto::system::PhysicalPlan const& _pplan) {
  multi_count_metric_->scope(METRIC_START_RESTORE)->incr();
  if (in_progress_) {
    multi_count_metric_->scope(METRIC_START_RESTORE_IN_PROGRESS)->incr();
    LOG(WARNING) << "Got a RestoreTopologyState request for " << _checkpoint_id
                 << " " << _restore_txid << " while we were still in old one "
                 << checkpoint_id_ << " " << restore_txid_;
    if (_restore_txid <= restore_txid_) {
      LOG(FATAL) << "New restore txid: " << _restore_txid << " is <= old "
                 << restore_txid_<< "!!! Dying!!! ";
    }
  } else {
    LOG(INFO) << "Starting Restore for checkpoint_id " << _checkpoint_id
              << " and txid " << _restore_txid;
    tuple_cache_->clear();
    clientmgr_->CloseConnectionsAndClear();
    server_->ClearCache();
    time_spent_metric_->Start();
  }
  // This is a new one for this checkpoint
  in_progress_ = true;
  clients_connections_pending_ = true;
  instance_connections_pending_ = true;
  local_taskids_ = _local_taskids;
  restore_pending_ = local_taskids_;
  get_ckpt_pending_ = local_taskids_;
  checkpoint_id_ = _checkpoint_id;
  restore_txid_ = _restore_txid;

  // Retreive checkpoints from the ckptmgr.
  GetCheckpoints();

  clientmgr_->StartConnections(_pplan);
  if (clientmgr_->AllStMgrClientsRegistered()) {
    // Its possible that this is really a restore while we were already in progress
    // and there was no change in pplan. In which case there would be no new
    // connections to restore
    LOG(INFO) << "All Stmgr have already connected to their peers in this restore";
    clients_connections_pending_ = false;
  }
  if (server_->HaveAllInstancesConnectedToUs()) {
    LOG(INFO) << "All Instances have already connected to us in this restore";
    instance_connections_pending_ = false;
  }
}

void StatefulRestorer::GetCheckpoints() {
  for (auto task_id : get_ckpt_pending_) {
    auto instance_info = server_->GetInstanceInfo(task_id);
    if (instance_info) {
      ckptmgr_->GetInstanceState(*instance_info, checkpoint_id_);
      multi_count_metric_->scope(METRIC_CKPT_REQUESTS)->incr();
    } else {
      LOG(ERROR) << "Could not send GetCheckpoint message for checkpoint "
                 << checkpoint_id_ << " for task " << task_id
                 << " because it is not connected to us";
    }
  }
}

void StatefulRestorer::HandleCheckpointState(proto::system::StatusCode _status, sp_int32 _task_id,
    sp_string _checkpoint_id,
    const proto::ckptmgr::InstanceStateCheckpoint& _state) {
  LOG(INFO) << "Got InstanceState from checkpoint mgr for task " << _task_id
            << " and checkpoint " << _state.checkpoint_id();
  multi_count_metric_->scope(METRIC_CKPT_RESPONSES)->incr();
  if (!in_progress_) {
    LOG(INFO) << "Ignoring InstanceState from " << _task_id << " for checkpoint "
              << _checkpoint_id << " because we are not in restore";
    multi_count_metric_->scope(METRIC_CKPT_RESPONSES_IGNORED)->incr();
    return;
  }
  if (_checkpoint_id != checkpoint_id_) {
    LOG(INFO) << "InstanceState from checkpont mgr for a checkpoint_id "
              << _checkpoint_id << " that is different from ours "
              << checkpoint_id_;
    multi_count_metric_->scope(METRIC_CKPT_RESPONSES_IGNORED)->incr();
    return;
  }
  if (_status == proto::system::OK) {
    if (_state.checkpoint_id() != checkpoint_id_) {
      LOG(WARNING) << "Discarding state retrieved from checkpoint mgr because the checkpoint"
                   << " id in the response does not match ours " << checkpoint_id_;
      multi_count_metric_->scope(METRIC_CKPT_RESPONSES_IGNORED)->incr();
      return;
    }
    if (server_->SendRestoreInstanceStateRequest(_task_id, _state)) {
      multi_count_metric_->scope(METRIC_INSTANCE_RESTORE_REQUESTS)->incr();
      get_ckpt_pending_.erase(_task_id);
    }
    // Note that if we are unable to send the restore instance state request(i.e. if the
    // last call returns false, its because of connection issues. When we officially detect
    // the issue(either in HandleRead/HandleWrite/HandleClose), we will be informed of a
    // dead instance and we be doing restore
  } else {
    LOG(INFO) << "InstanceState from checkpont mgr contained non ok status " << _status;
    in_progress_ = false;
    time_spent_metric_->Stop();
    multi_count_metric_->scope(METRIC_CKPT_RESPONSES_ERROR)->incr();
    multi_count_metric_->scope(METRIC_START_RESTORE_FAILED)->incr();
    restore_done_watcher_(_status, checkpoint_id_, restore_txid_);
  }
}

void StatefulRestorer::HandleInstanceRestoredState(sp_int32 _task_id,
                                                   const proto::system::StatusCode _status,
                                                   const std::string& _checkpoint_id) {
  LOG(INFO) << "Instance " << _task_id << " restored its state for " << _checkpoint_id
            << " with status " << _status;

  // We don't handle well if instances are not able to restore
  // TODO(skukarni) Fix this
  CHECK_EQ(_status, proto::system::OK);

  multi_count_metric_->scope(METRIC_INSTANCE_RESTORE_RESPONSES)->incr();
  if (!in_progress_) {
    LOG(INFO) << "Ignoring the Instance Restored State for task " << _task_id
              << " and checkpoint " << _checkpoint_id << " because we are not in Restore";
    multi_count_metric_->scope(METRIC_INSTANCE_RESTORE_RESPONSES_IGNORED)->incr();
    return;
  }
  if (_checkpoint_id != checkpoint_id_) {
    LOG(WARNING) << "Ignoring it because we are operating on a different one " << checkpoint_id_;
    multi_count_metric_->scope(METRIC_INSTANCE_RESTORE_RESPONSES_IGNORED)->incr();
    return;
  }
  restore_pending_.erase(_task_id);
  CheckAndFinishRestore();
}

void StatefulRestorer::HandleCkptMgrRestart() {
  LOG(INFO) << "Checkpoint ClientMgr restarted";
  if (in_progress_) {
    GetCheckpoints();
  }
}

void StatefulRestorer::HandleAllStMgrClientsConnected() {
  LOG(INFO) << "All StMgr Clients Connected";
  if (!in_progress_) {
    LOG(INFO) << "Ignoring it becuase we are not in restore";
    return;
  }
  clients_connections_pending_ = false;
  CheckAndFinishRestore();
}

void StatefulRestorer::HandleDeadStMgrConnection() {
  if (in_progress_) {
    clients_connections_pending_ = true;
  }
}

void StatefulRestorer::HandleAllInstancesConnected() {
  LOG(INFO) << "All Instances Connected";
  if (!in_progress_) {
    LOG(INFO) << "Ignoring it becuase we are not in restore";
    return;
  }
  instance_connections_pending_ = false;
  if (!get_ckpt_pending_.empty()) {
    GetCheckpoints();
  } else {
    CheckAndFinishRestore();
  }
}

void StatefulRestorer::HandleDeadInstanceConnection(sp_int32 _task_id) {
  if (in_progress_) {
    instance_connections_pending_ = true;
    CHECK(local_taskids_.find(_task_id) != local_taskids_.end());
    restore_pending_.insert(_task_id);
    get_ckpt_pending_.insert(_task_id);
  }
}

void StatefulRestorer::CheckAndFinishRestore() {
  if (!instance_connections_pending_ && !clients_connections_pending_ &&
      restore_pending_.empty()) {
    LOG(INFO) << "Restore Done Successfully for " << checkpoint_id_
              << " " << restore_txid_;
    in_progress_ = false;
    time_spent_metric_->Stop();
    restore_done_watcher_(proto::system::OK, checkpoint_id_, restore_txid_);
  }
}

}  // namespace stmgr
}  // namespace heron
