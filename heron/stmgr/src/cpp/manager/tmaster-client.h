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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMANAGER_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMANAGER_CLIENT_H_

#include <set>
#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace stmgr {

using std::shared_ptr;

class TManagerClient : public Client {
 public:
  TManagerClient(shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                const sp_string& _stmgr_id,
                const sp_string& _stmgr_host, sp_int32 _data_port, sp_int32 _local_data_port,
                sp_int32 _shell_port,
                VCallback<shared_ptr<proto::system::PhysicalPlan>> _pplan_watch,
                VCallback<sp_string> _stateful_checkpoint_watch,
                VCallback<sp_string, sp_int64> _restore_topology_watch,
                VCallback<sp_string> _start_stateful_watch,
                VCallback<const proto::ckptmgr::StatefulConsistentCheckpointSaved&>
                    _broadcast_checkpoint_saved);
  virtual ~TManagerClient();

  // Told by the upper layer to disconnect and self destruct
  void Die();

  // Sets the instances that belong to us
  void SetInstanceInfo(const std::vector<proto::system::Instance*>& _instances);

  // returns the tmanager address "host:port" form.
  sp_string getTmanagerHostPort();

  // Send a InstanceStateStored message to tmanager
  void SavedInstanceState(const proto::system::Instance& _instance,
                          const std::string& _checkpoint_id);

  // Send RestoreTopologyStateResponse to tmanager
  void SendRestoreTopologyStateResponse(proto::system::StatusCode _status,
                                        const std::string& _checkpoint_id,
                                        sp_int64 _txid);

  // Send ResetTopologyState message to tmanager
  void SendResetTopologyState(const std::string& _dead_stmgr,
                              int32_t _dead_instance,
                              const std::string& _reason);

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterResponse(void*,
                              pool_unique_ptr<proto::tmanager::StMgrRegisterResponse> _response,
                              NetworkErrorCode);
  void HandleHeartbeatResponse(void*,
                               pool_unique_ptr<proto::tmanager::StMgrHeartbeatResponse> response,
                               NetworkErrorCode);

  void HandleNewAssignmentMessage(pool_unique_ptr<proto::stmgr::NewPhysicalPlanMessage> _message);
  void HandleStatefulCheckpointMessage(
          pool_unique_ptr<proto::ckptmgr::StartStatefulCheckpoint> _message);
  void HandleRestoreTopologyStateRequest(
          pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateRequest> _message);
  void HandleStartStmgrStatefulProcessing(
          pool_unique_ptr<proto::ckptmgr::StartStmgrStatefulProcessing> _msg);

  void HandleStatefulCheckpointSavedMessage(
          pool_unique_ptr<proto::ckptmgr::StatefulConsistentCheckpointSaved> _msg);

  void OnReConnectTimer();
  void OnHeartbeatTimer();
  void SendRegisterRequest();
  void SendHeartbeatRequest();

  void CleanInstances();

  sp_string stmgr_id_;
  sp_string stmgr_host_;
  sp_int32 data_port_;
  sp_int32 local_data_port_;
  sp_int32 shell_port_;

  // Set of instances to be reported to tmanager
  std::set<unique_ptr<proto::system::Instance>> instances_;

  bool to_die_;
  // We invoke this callback upon a new physical plan from tmanager
  VCallback<shared_ptr<proto::system::PhysicalPlan>> pplan_watch_;
  // We invoke this callback upon receiving a checkpoint message from tmanager
  // passing in the checkpoint id
  VCallback<sp_string> stateful_checkpoint_watch_;
  // We invoke this callback upon receiving a restore topology message from tmanager
  // passing in the checkpoint id and the txid
  VCallback<sp_string, sp_int64> restore_topology_watch_;
  // We invoke this callback upon receiving a StartStatefulProcessing message from tmanager
  // passing in the checkpoint id
  VCallback<sp_string> start_stateful_watch_;
  // This callback will be invoked upon receiving a StatefulConsistentCheckpointSaved message.
  // We will then forward this message to all the instances connected to this stmgr
  VCallback<const proto::ckptmgr::StatefulConsistentCheckpointSaved&> broadcast_checkpoint_saved_;

  // Configs to be read
  sp_int32 reconnect_tmanager_interval_sec_;
  sp_int32 stream_to_tmanager_heartbeat_interval_sec_;

  sp_int64 reconnect_timer_id;
  sp_int64 heartbeat_timer_id;

  // Counter for reconnect attempts
  sp_int32 reconnect_attempts_;
  sp_int32 reconnect_max_attempt_;

  // Permanent timer callbacks
  VCallback<> reconnect_timer_cb;
  VCallback<> heartbeat_timer_cb;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMANAGER_CLIENT_H_
