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

#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_

#include <string>
#include "basics/basics.h"
#include "network/network.h"
#include "network/network_error.h"
#include "proto/messages.h"

namespace heron {
namespace stmgr {

class CkptMgrClient : public Client {
 public:
  CkptMgrClient(EventLoop* eventLoop, const NetworkOptions& _options,
                const sp_string& _topology_name, const sp_string& _topology_id,
                const sp_string& _our_ckptmgr_id, const sp_string& _our_stmgr_id,
                std::function<void(const proto::system::Instance&,
                                   const std::string&)> _ckpt_saved_watcher,
                std::function<void(proto::system::StatusCode, sp_int32, sp_string,
                              const proto::ckptmgr::InstanceStateCheckpoint&)> _ckpt_get_watcher,
                std::function<void()> _register_watcher);
  virtual ~CkptMgrClient();

  void Quit();

  // TODO(nlu): add requests methods
  virtual void SaveInstanceState(proto::ckptmgr::SaveInstanceStateRequest* _request);
  virtual void GetInstanceState(const proto::system::Instance& _instance,
                                const std::string& _checkpoint_id);

 protected:
  void GetInstanceState(const proto::system::Instance& _instance,
                        const std::string& _checkpoint_id, int32_t* _nattempts);
  virtual void HandleSaveInstanceStateResponse(void*,
                             proto::ckptmgr::SaveInstanceStateResponse* _response,
                             NetworkErrorCode status);
  virtual void HandleGetInstanceStateResponse(void*,
                             proto::ckptmgr::GetInstanceStateResponse* _response,
                             NetworkErrorCode status);
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterStMgrResponse(void *, proto::ckptmgr::RegisterStMgrResponse *_response,
                                   NetworkErrorCode);
  void SendRegisterRequest();

  void OnReconnectTimer();

  // TODO(nlu): add response handler methods

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
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_CKPTMGR_SRC_CKPTCLIENT_CLIENT_H_

