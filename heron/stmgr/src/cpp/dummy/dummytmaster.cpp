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

#include <iostream>
#include <sstream>

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "state/heron-statemgr.h"
#include "config/helper.h"

#include "dummytmaster.h"

namespace heron {
namespace tmaster {

TMasterServer::TMasterServer(EventLoop* eventLoop, const NetworkOptions& _options,
                             const sp_string& _topology_name, const sp_string& _zkhostport,
                             const sp_string& _zkroot, const std::vector<sp_string>& _stmgrs)
    : Server(eventLoop, _options), pplan_(NULL), stmgrs_(_stmgrs) {
  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(_zkhostport, _zkroot, eventLoop);
  topology_ = new proto::api::Topology();
  state_mgr_->GetTopology(_topology_name, topology_,
                          CreateCallback(this, &TMasterServer::OnTopologyFetch));
}

void TMasterServer::OnTopologyFetch(proto::system::StatusCode _status) {
  CHECK(_status == proto::system::OK);
  std::cout << "Topology id is " << topology_->id() << std::endl;
  GeneratePplan();
  proto::tmaster::TMasterLocation tmaster_location;
  tmaster_location.set_topology_name(topology_->name());
  tmaster_location.set_topology_id(topology_->id());
  tmaster_location.set_host(get_serveroptions().get_host());
  tmaster_location.set_controller_port(-1);  // dummy does not support control
  tmaster_location.set_master_port(get_serveroptions().get_port());
  state_mgr_->SetTMasterLocation(tmaster_location,
                                 CreateCallback(this, &TMasterServer::OnTMasterSet));
}

void TMasterServer::OnTMasterSet(proto::system::StatusCode _status) {
  CHECK(_status == proto::system::OK);
  // Install the handlers
  InstallRequestHandler(&TMasterServer::HandleStMgrRegisterRequest);
  InstallRequestHandler(&TMasterServer::HandleStMgrHeartbeatRequest);
  Start();
}

TMasterServer::~TMasterServer() {
  delete topology_;
  delete pplan_;
}

void TMasterServer::GeneratePplan() {
  pplan_ = new proto::system::PhysicalPlan();
  pplan_->mutable_topology()->CopyFrom(*topology_);
  std::vector<sp_string> stmgr_ids;
  for (size_t i = 0; i < stmgrs_.size(); ++i) {
    std::vector<sp_string> components = StrUtils::split(stmgrs_[i], ":");
    CHECK(components.size() == 3);
    stmgr_ids.push_back(components[0]);
    proto::system::StMgr* stmgr = pplan_->add_stmgrs();
    stmgr->set_id(components[0]);
    stmgr->set_host_name(components[1]);
    stmgr->set_data_port(atoi(components[2].c_str()));
    stmgr->set_local_endpoint("/tmp/unused");
  }
  // Basically divide all executors evenly
  size_t index = 0;
  sp_int32 global_index = 0;
  for (sp_int32 i = 0; i < topology_->spouts_size(); ++i) {
    const proto::api::Spout& spout = topology_->spouts(i);
    sp_int32 parallelism =
        heron::config::TopologyConfigHelper::GetComponentParallelism(spout.comp().config());
    for (sp_int32 j = 0; j < parallelism; ++j) {
      proto::system::Instance* instance = pplan_->add_instances();
      std::ostringstream w;
      w << "instance-" << global_index;
      instance->set_instance_id(w.str());
      instance->set_stmgr_id(stmgr_ids[index]);
      std::cout << "Worker " << w.str() << " goes to " << stmgr_ids[index] << std::endl;
      if (++index >= stmgr_ids.size()) {
        index = 0;
      }
      proto::system::InstanceInfo* i = instance->mutable_info();
      i->set_task_id(global_index);
      i->set_component_index(j);
      i->set_component_name(spout.comp().name());
      global_index++;
    }
  }
  for (sp_int32 i = 0; i < topology_->bolts_size(); ++i) {
    const proto::api::Bolt& bolt = topology_->bolts(i);
    sp_int32 parallelism =
        heron::config::TopologyConfigHelper::GetComponentParallelism(bolt.comp().config());
    for (sp_int32 j = 0; j < parallelism; ++j) {
      proto::system::Instance* instance = pplan_->add_instances();
      std::ostringstream w;
      w << "instance-" << global_index;
      instance->set_instance_id(w.str());
      instance->set_stmgr_id(stmgr_ids[index]);
      std::cout << "Worker " << w.str() << " goes to " << stmgr_ids[index] << std::endl;
      if (++index >= stmgr_ids.size()) {
        index = 0;
      }
      proto::system::InstanceInfo* i = instance->mutable_info();
      i->set_task_id(global_index);
      i->set_component_index(j);
      i->set_component_name(bolt.comp().name());
      global_index++;
    }
  }
}

void TMasterServer::HandleNewConnection(Connection*) {
  // There is nothing to be done here. Instead we wait for
  // the register message
}

void TMasterServer::HandleConnectionClose(Connection*, NetworkErrorCode) {
  // Nothing that we do here
}

void TMasterServer::HandleStMgrRegisterRequest(REQID _reqid, Connection* _conn,
                                               proto::tmaster::StMgrRegisterRequest* _request) {
  delete _request;
  proto::tmaster::StMgrRegisterResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  response.mutable_pplan()->CopyFrom(*pplan_);
  SendResponse(_reqid, _conn, response);
}

void TMasterServer::HandleStMgrHeartbeatRequest(REQID _reqid, Connection* _conn,
                                                proto::tmaster::StMgrHeartbeatRequest* _request) {
  delete _request;
  proto::tmaster::StMgrHeartbeatResponse response;
  response.mutable_status()->set_status(proto::system::OK);
  SendResponse(_reqid, _conn, response);
}
}
}  // end of namespace
