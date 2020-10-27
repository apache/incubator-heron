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

#include "manager/stmgrstate.h"
#include <iostream>
#include <string>
#include <vector>
#include "manager/tmanagerserver.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace tmanager {

StMgrState::StMgrState(Connection* _conn, const proto::system::StMgr& _stmgr,
                       const std::vector<shared_ptr<proto::system::Instance>>& _instances,
                       Server& _server) : server_(_server) {
  last_heartbeat_ = time(NULL);
  last_stats_ = NULL;
  instances_ = _instances;
  stmgr_ = std::make_shared<proto::system::StMgr>(_stmgr);
  connection_ = _conn;
}

StMgrState::~StMgrState() {
  delete last_stats_;
}

void StMgrState::UpdateWithNewStMgr(const proto::system::StMgr& _stmgr,
                                const std::vector<shared_ptr<proto::system::Instance>>& _instances,
                                Connection* _conn) {
  delete last_stats_;
  last_stats_ = NULL;
  stmgr_ = std::make_shared<proto::system::StMgr>(_stmgr);
  instances_ = _instances;
  connection_ = _conn;
}

bool StMgrState::VerifyInstances(const std::vector<proto::system::Instance*>& _instances) {
  if (instances_.size() != _instances.size()) return false;
  for (size_t i = 0; i < instances_.size(); ++i) {
    bool found = false;
    for (size_t j = 0; j < _instances.size(); ++j) {
      if (instances_[i]->instance_id() != _instances[j]->instance_id()) continue;
      if (instances_[i]->stmgr_id() != _instances[j]->stmgr_id()) continue;
      if (instances_[i]->info().task_id() != _instances[j]->info().task_id()) continue;
      if (instances_[i]->info().component_index() != _instances[j]->info().component_index())
        continue;
      if (instances_[i]->info().component_name() != _instances[j]->info().component_name())
        continue;
      found = true;
      break;
    }
    if (!found) return false;
  }
  return true;
}

void StMgrState::heartbeat(sp_int64, proto::system::StMgrStats* _stats) {
  // Right now we ignore the time supplied by the stmgr.
  // TODO(kramasamy): Figure out the right way here
  last_heartbeat_ = time(NULL);
  delete last_stats_;
  last_stats_ = _stats;
}

void StMgrState::SendRestoreTopologyStateMessage(
            const proto::ckptmgr::RestoreTopologyStateRequest& _message) {
  LOG(INFO) << "Sending restore topology state message to stmgr " << stmgr_->id()
            << " with checkpoint " << _message.checkpoint_id();
  server_.SendMessage(connection_, _message);
}

void StMgrState::SendStartStatefulProcessingMessage(const std::string& _checkpoint_id) {
  LOG(INFO) << "Sending Start Stateful Processing message to stmgr " << stmgr_->id()
            << " with checkpoint " << _checkpoint_id;
  proto::ckptmgr::StartStmgrStatefulProcessing message;
  message.set_checkpoint_id(_checkpoint_id);
  server_.SendMessage(connection_, message);
}

void StMgrState::NewPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  LOG(INFO) << "Sending a new physical plan to stmgr " << stmgr_->id();
  proto::stmgr::NewPhysicalPlanMessage message;
  message.mutable_new_pplan()->CopyFrom(_pplan);
  server_.SendMessage(connection_, message);
}

void StMgrState::NewStatefulCheckpoint(const proto::ckptmgr::StartStatefulCheckpoint& _request) {
  LOG(INFO) << "Sending a new stateful checkpoint request to stmgr: " << stmgr_->id();
  server_.SendMessage(connection_, _request);
}

void StMgrState::SendCheckpointSavedMessage(
        const proto::ckptmgr::StatefulConsistentCheckpointSaved &_msg) {
  LOG(INFO) << "Sending checkpoint saved message to stmgr: " << stmgr_->id() << " "
            << "for checkpoint: " << _msg.consistent_checkpoint().checkpoint_id();
  server_.SendMessage(connection_, _msg);
}

/*
void
StMgrState::AddAssignment(const std::vector<pair<string, sp_int32> >& _assignments,
                                proto::system::Assignment* _assignment)
{
  // A vector of <component_id, partition_id is given to us.
  CHECK(_assignments.size() == workers_.size());

  proto::system::NodeManagerAssignment* val = _assignment->add_assignments();
  val->set_nodemgr_id(info_->nodemgr_id());
  set<string>::iterator iter = workers_.begin();
  for (sp_uint32 i = 0; i < _assignments.size(); ++i) {
    proto::system::WorkerAssignment* wrkr = val->add_workers();
    wrkr->set_worker_id(*iter);
    wrkr->set_comp_id(_assignments[i].first);
    wrkr->set_instance_id(_assignments[i].second);
    ++iter;
  }
}
*/

bool StMgrState::TimedOut() const {
  sp_int32 timeout =
      config::HeronInternalsConfigReader::Instance()->GetHeronTmanagerStmgrStateTimeoutSec();
  return (time(NULL) - last_heartbeat_) > timeout;
}
}  // namespace tmanager
}  // namespace heron
