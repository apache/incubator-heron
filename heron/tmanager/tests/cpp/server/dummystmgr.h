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

#ifndef __DUMMYSTMGR_H_
#define __DUMMYSTMGR_H_

#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace testing {

class DummyStMgr : public Client {
 public:
  DummyStMgr(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
             const sp_string& stmgr_id,
             const sp_string& myhost, sp_int32 myport,
             const std::vector<proto::system::Instance*>& instances);
  ~DummyStMgr();

  proto::system::PhysicalPlan* GetPhysicalPlan();
  bool GotRestoreMessage() const { return got_restore_message_; }
  void ResetGotRestoreMessage() { got_restore_message_ = false; }
  bool GotStartProcessingMessage() const { return got_start_message_; }
  void ResetGotStartProcessingMessage() { got_start_message_ = false; }
  const std::string& stmgrid() const { return my_id_; }

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterResponse(void*,
                              pool_unique_ptr<proto::tmanager::StMgrRegisterResponse> response,
                              NetworkErrorCode);
  void HandleHeartbeatResponse(void*,
                               pool_unique_ptr<proto::tmanager::StMgrHeartbeatResponse> response,
                               NetworkErrorCode);
  void HandleNewAssignmentMessage(pool_unique_ptr<proto::stmgr::NewPhysicalPlanMessage> message);
  void HandleNewPhysicalPlan(const proto::system::PhysicalPlan& pplan);
  void HandleRestoreTopologyStateRequest(
          pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateRequest> message);
  void HandleStartProcessingMessage(
          pool_unique_ptr<proto::ckptmgr::StartStmgrStatefulProcessing> message);

  void OnReConnectTimer();
  void OnHeartbeatTimer();
  void SendRegisterRequest();
  void SendHeartbeatRequest();

  std::string my_id_;
  std::string my_host_;
  sp_int32 my_port_;
  std::vector<proto::system::Instance*> instances_;

  proto::system::PhysicalPlan* pplan_;
  bool got_restore_message_;
  bool got_start_message_;
};
}  // namespace testing
}  // namespace heron
#endif
