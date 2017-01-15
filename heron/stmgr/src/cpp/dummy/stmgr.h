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

#ifndef __TCONTROLLER_H_
#define __TCONTROLLER_H_

#include <set>
#include <vector>
#include "network/network_error.h"

namespace heron {
namespace stmgr {

class StMgr : public Server {
 public:
  StMgr(EventLoop* eventLoop, const NetworkOptions& options, const sp_string& _topology_name,
        const sp_string& _stmgr_id, const std::vector<sp_string>& _spout_instances,
        const std::vector<sp_string>& _bolt_instances, const sp_string& _zkhostport,
        const sp_string& _zkroot);
  virtual ~StMgr();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests
  void HandleRegisterInstanceRequest(REQID _id, Connection* _conn,
                                     proto::stmgr::RegisterInstanceRequest* _request);
  void HandleTupleSetMessage(Connection* _conn, proto::stmgr::TupleMessage* _message);

  void OnTopologyFetch(proto::api::Topology* _topology, proto::system::StatusCode);
  heron::proto::system::PhysicalPlan* GeneratePhysicalPlan(proto::api::Topology* _topology);
  void SendSpoutMessageToBolt(proto::stmgr::TupleMessage* _message);

  std::vector<Connection*> spout_connections_;
  std::vector<Connection*> bolt_connections_;
  heron::common::HeronStateMgr* state_mgr_;
  proto::system::PhysicalPlan* pplan_;
  sp_string stmgr_id_;
  sp_int32 stmgr_port_;
  std::set<sp_string> spout_instances_;
  std::set<sp_string> bolt_instances_;
  size_t spout_index_;
  size_t bolt_index_;
};
}
}  // end namespace

#endif
