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

#ifndef SRC_CPP_SVCS_TMANAGER_SRC_CKPTMGR_CLIENT_H
#define SRC_CPP_SVCS_TMANAGER_SRC_CKPTMGR_CLIENT_H

#include <string>
#include "basics/basics.h"
#include "network/network.h"
#include "network/network_error.h"
#include "proto/messages.h"

namespace heron {
namespace tmanager {

class CkptMgrClient : public Client {
 public:
  CkptMgrClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                const sp_string& _topology_name, const sp_string& _topology_id,
                std::function<void(proto::system::StatusCode)> _clean_response_watcher);
  virtual ~CkptMgrClient();

  void Quit();

  void SendCleanStatefulCheckpointRequest(const std::string& _oldest_ckpt, bool _clean_all);

 protected:
  virtual void HandleCleanStatefulCheckpointResponse(
                        void*,
                        pool_unique_ptr<proto::ckptmgr::CleanStatefulCheckpointResponse> _response,
                        NetworkErrorCode status);
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleTManagerRegisterResponse(
                                  void*,
                                  pool_unique_ptr<proto::ckptmgr::RegisterTManagerResponse>_response,
                                  NetworkErrorCode _status);

  void SendRegisterRequest();

  void OnReconnectTimer();

  sp_string topology_name_;
  sp_string topology_id_;
  bool quit_;
  std::function<void(proto::system::StatusCode)> clean_response_watcher_;

  // Config value for reconnect interval
  sp_int32 reconnect_ckptmgr_interval_sec_;
};

}  // namespace tmanager
}  // namespace heron

#endif  // SRC_CPP_SVCS_TMANAGER_SRC_CKPTMGR_CLIENT_H
