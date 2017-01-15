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

#ifndef __TMASTERSERVER_H
#define __TMASTERSERVER_H

#include <vector>
#include "network/network_error.h"

namespace heron {
namespace tmaster {

class TMasterServer : public Server {
 public:
  TMasterServer(EventLoop* eventLoop, const NetworkOptions& options,
                const sp_string& _topology_name, const sp_string& _zkhostport,
                const sp_string& _zkroot, const std::vector<sp_string>& _stmgrids);
  virtual ~TMasterServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests
  void HandleStMgrRegisterRequest(REQID _id, Connection* _conn,
                                  proto::tmaster::StMgrRegisterRequest* _request);
  void HandleStMgrHeartbeatRequest(REQID _id, Connection* _conn,
                                   proto::tmaster::StMgrHeartbeatRequest* _request);

  void OnTopologyFetch(proto::system::StatusCode _status);
  void OnTMasterSet(proto::system::StatusCode _status);
  void GeneratePplan();

  proto::api::Topology* topology_;
  proto::system::PhysicalPlan* pplan_;
  std::vector<sp_string> stmgrs_;
  heron::common::HeronStateMgr* state_mgr_;
};
}
}  // end namespace

#endif
