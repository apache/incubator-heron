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

#ifndef __STMANAGERSTATE_H
#define __STMANAGERSTATE_H

#include <string>
#include <vector>
#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "proto/ckptmgr.pb.h"
#include "basics/basics.h"

namespace heron {
namespace proto {
namespace system {
class StMgr;
class StMgrStats;
class PhysicalPlan;
}
}
}

namespace heron {
namespace tmanager {

using std::shared_ptr;

class TManagerServer;

class StMgrState {
 public:
  StMgrState(Connection* _conn, const proto::system::StMgr& _info,
             const std::vector<shared_ptr<proto::system::Instance>>& _instances, Server& _server);
  virtual ~StMgrState();

  void UpdateWithNewStMgr(const proto::system::StMgr& _info,
                          const std::vector<shared_ptr<proto::system::Instance>>& _instances,
                          Connection* _conn);

  // Update the heartbeat. Note:- We own _stats now
  void heartbeat(sp_int64 _time, proto::system::StMgrStats* _stats);

  // Send messages to the stmgr
  void NewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);


  // Send RestoreTopologyStateMessage to stmgr
  void SendRestoreTopologyStateMessage(const proto::ckptmgr::RestoreTopologyStateRequest& _message);

  // Send StartStatefulProcessingMessage to stmgr
  void SendStartStatefulProcessingMessage(const std::string& _checkpoint_id);

  // Send stateful checkpoint message to the stmgr
  void NewStatefulCheckpoint(const proto::ckptmgr::StartStatefulCheckpoint& _request);

  void SendCheckpointSavedMessage(const proto::ckptmgr::StatefulConsistentCheckpointSaved &_msg);


  bool TimedOut() const;

  // getters
  Connection* get_connection() { return connection_; }
  const std::string& get_id() const { return stmgr_->id(); }
  sp_uint32 get_num_instances() const { return instances_.size(); }
  const std::vector<shared_ptr<proto::system::Instance>>& get_instances() const {return instances_;}
  const shared_ptr<proto::system::StMgr> get_stmgr() const { return stmgr_; }
  bool VerifyInstances(const std::vector<proto::system::Instance*>& _instances);

 private:
  // The last time we got a hearbeat from this stmgr
  sp_int64 last_heartbeat_;
  // The stats that was reported last time
  proto::system::StMgrStats* last_stats_;

  // All the instances on this stmgr
  std::vector<shared_ptr<proto::system::Instance>> instances_;

  // The info about this stmgr
  shared_ptr<proto::system::StMgr> stmgr_;
  // The connection used by the nodemanager to contact us
  Connection* connection_;
  // Our link to our TManager
  Server& server_;
};
}  // namespace tmanager
}  // namespace heron

#endif
