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

#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_

#include <string>
#include "basics/basics.h"
#include "network/network.h"
#include "network/network_error.h"
#include "proto/messages.h"

namespace heron {
namespace stmgr {

using std::unique_ptr;

class CkptMgrClient : public Client {
 public:
  CkptMgrClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                const sp_string& _topology_name, const sp_string& _topology_id,
                const sp_string& _our_ckptmgr_id, const sp_string& _our_stmgr_id,
                std::function<void(const proto::system::Instance&,
                                   const std::string&)> _ckpt_saved_watcher,
                std::function<void(proto::system::StatusCode, sp_int32, sp_string,
                              const proto::ckptmgr::InstanceStateCheckpoint&)>
                              _ckpt_get_watcher,
                std::function<void()> _register_watcher);
  virtual ~CkptMgrClient();

  void Quit();

  virtual void SaveInstanceState(unique_ptr<proto::ckptmgr::SaveInstanceStateRequest> _request);
  virtual void GetInstanceState(const proto::system::Instance& _instance,
                                const std::string& _checkpoint_id);
  virtual void SetPhysicalPlan(proto::system::PhysicalPlan& _pplan);

 protected:
  void GetInstanceState(const proto::system::Instance& _instance,
                        const std::string& _checkpoint_id, int32_t* _nattempts);
  virtual void HandleSaveInstanceStateResponse(void*,
                             pool_unique_ptr<proto::ckptmgr::SaveInstanceStateResponse> _response,
                             NetworkErrorCode status);
  virtual void HandleGetInstanceStateResponse(void*,
                             pool_unique_ptr<proto::ckptmgr::GetInstanceStateResponse> _response,
                             NetworkErrorCode status);
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterStMgrResponse(
                                   void *,
                                   pool_unique_ptr<proto::ckptmgr::RegisterStMgrResponse >_response,
                                   NetworkErrorCode);
  void SendRegisterRequest();

  void OnReconnectTimer();

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string ckptmgr_id_;
  sp_string stmgr_id_;
  bool quit_;
  std::function<void(const proto::system::Instance&,
                     const std::string&)> ckpt_saved_watcher_;
  std::function<void(proto::system::StatusCode, sp_int32, sp_string,
                     const proto::ckptmgr::InstanceStateCheckpoint&)> ckpt_get_watcher_;
  std::function<void()> register_watcher_;

  // Config
  sp_int32 reconnect_cpktmgr_interval_sec_;

  proto::system::PhysicalPlan* pplan_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_

