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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMASTER_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMASTER_CLIENT_H_

#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace stmgr {

class TMasterClient : public Client {
 public:
  TMasterClient(EventLoop* eventLoop, const NetworkOptions& _options, const sp_string& _stmgr_id,
                const sp_string& _stmgr_host, sp_int32 _stmgr_port, sp_int32 _shell_port,
                VCallback<proto::system::PhysicalPlan*> _pplan_watch,
                VCallback<sp_string> _stateful_checkpoint_watch,
                VCallback<sp_string, sp_int64> _restore_topology_watch,
                VCallback<sp_string> _start_stateful_watch);
  virtual ~TMasterClient();

  // Told by the upper layer to disconnect and self destruct
  void Die();

  // Sets the instances that belong to us
  void SetInstanceInfo(const std::vector<proto::system::Instance*>& _instances) {
    instances_ = _instances;
  }

  // returns the tmaster address "host:port" form.
  sp_string getTmasterHostPort();

  // Send a InstanceStateStored message to tmaster
  void SavedInstanceState(const proto::system::Instance& _instance,
                          const std::string& _checkpoint_id);

  // Send RestoreTopologyStateResponse to tmaster
  void SendRestoreTopologyStateResponse(proto::system::StatusCode _status,
                                        const std::string& _checkpoint_id,
                                        sp_int64 _txid);

  // Send ResetTopologyState message to tmaster
  void SendResetTopologyState(const std::string& _dead_stmgr,
                              int32_t _dead_instance,
                              const std::string& _reason);

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterResponse(void*, proto::tmaster::StMgrRegisterResponse* _response,
                              NetworkErrorCode);
  void HandleHeartbeatResponse(void*, proto::tmaster::StMgrHeartbeatResponse* response,
                               NetworkErrorCode);

  void HandleNewAssignmentMessage(proto::stmgr::NewPhysicalPlanMessage* _message);
  void HandleStatefulCheckpointMessage(proto::ckptmgr::StartStatefulCheckpoint* _message);
  void HandleRestoreTopologyStateRequest(proto::ckptmgr::RestoreTopologyStateRequest* _message);
  void HandleStartStmgrStatefulProcessing(proto::ckptmgr::StartStmgrStatefulProcessing* _msg);

  void OnReConnectTimer();
  void OnHeartbeatTimer();
  void SendRegisterRequest();
  void SendHeartbeatRequest();

  sp_string stmgr_id_;
  sp_string stmgr_host_;
  sp_int32 stmgr_port_;
  sp_int32 shell_port_;
  std::vector<proto::system::Instance*> instances_;
  bool to_die_;
  // We invoke this callback upon a new physical plan from tmaster
  VCallback<proto::system::PhysicalPlan*> pplan_watch_;
  // We invoke this callback upon receiving a checkpoint message from tmaster
  // passing in the checkpoint id
  VCallback<sp_string> stateful_checkpoint_watch_;
  // We invoke this callback upon receiving a restore topology message from tmaster
  // passing in the checkpoint id and the txid
  VCallback<sp_string, sp_int64> restore_topology_watch_;
  // We invoke this callback upon receiving a StartStatefulProcessing message from tmaster
  // passing in the checkpoint id
  VCallback<sp_string> start_stateful_watch_;

  // Configs to be read
  sp_int32 reconnect_tmaster_interval_sec_;
  sp_int32 stream_to_tmaster_heartbeat_interval_sec_;

  sp_int64 reconnect_timer_id;
  sp_int64 heartbeat_timer_id;

  // Permanent timer callbacks
  VCallback<> reconnect_timer_cb;
  VCallback<> heartbeat_timer_cb;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_TMASTER_CLIENT_H_
