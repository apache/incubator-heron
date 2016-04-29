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
#include <map>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network//network.h"

#include "state/heron-statemgr.h"
#include "dummy/stmgr.h"

namespace heron {
namespace stmgr {

sp_int64 lrand() {
  sp_int64 retval = static_cast<sp_int64>(rand());
  return retval << (sizeof(int) * 8) | rand();
}

StMgr::StMgr(EventLoop* eventLoop, const NetworkOptions& _options, const sp_string& _topology_name,
             const sp_string& _stmgr_id, const std::vector<sp_string>& _spout_instances,
             const std::vector<sp_string>& _bolt_instances, const sp_string& _zkhostport,
             const sp_string& _zkroot)
    : Server(eventLoop, _options), stmgr_id_(_stmgr_id), spout_index_(0), bolt_index_(0) {
  stmgr_port_ = _options.get_port();
  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(_zkhostport, _zkroot, eventLoop);
  pplan_ = NULL;
  proto::api::Topology* topo = new proto::api::Topology();

  for (size_t i = 0; i < _spout_instances.size(); ++i) {
    spout_instances_.insert(_spout_instances[i]);
  }
  for (size_t i = 0; i < _bolt_instances.size(); ++i) {
    bolt_instances_.insert(_bolt_instances[i]);
  }
  state_mgr_->GetTopology(_topology_name, topo,
                          CreateCallback(this, &StMgr::OnTopologyFetch, topo));
}

void StMgr::OnTopologyFetch(proto::api::Topology* _topology, proto::system::StatusCode _status) {
  CHECK(_status == proto::system::OK);
  pplan_ = GeneratePhysicalPlan(_topology);
  delete _topology;

  // Install the handlers
  InstallRequestHandler(&StMgr::HandleRegisterInstanceRequest);
  InstallMessageHandler(&StMgr::HandleTupleSetMessage);
  Start();
}

heron::proto::system::PhysicalPlan* StMgr::GeneratePhysicalPlan(
    heron::proto::api::Topology* _topology) {
  heron::proto::system::PhysicalPlan* pplan = new heron::proto::system::PhysicalPlan();
  // Copy the topology verbatim
  pplan->mutable_topology()->CopyFrom(*_topology);

  // Just one stmgr
  heron::proto::system::StMgr* stmgr = pplan->add_stmgrs();
  stmgr->set_id(stmgr_id_);
  stmgr->set_host_name("127.0.0.1");
  stmgr->set_data_port(stmgr_port_);
  stmgr->set_local_endpoint("/notused");

  // Loop over spouts
  sp_int32 global_index = 0;
  sp_int32 instance_index = 0;
  for (std::set<sp_string>::iterator iter = spout_instances_.begin();
       iter != spout_instances_.end(); ++iter) {
    heron::proto::system::Instance* wrkr = pplan->add_instances();
    wrkr->set_instance_id(*iter);
    wrkr->set_stmgr_id(stmgr_id_);
    heron::proto::system::InstanceInfo* instance = wrkr->mutable_info();
    instance->set_task_id(global_index++);
    instance->set_component_index(instance_index++);
    instance->set_component_name("word");
  }

  instance_index = 0;
  // Loop over bolts
  for (std::set<sp_string>::iterator iter = bolt_instances_.begin(); iter != bolt_instances_.end();
       ++iter) {
    heron::proto::system::Instance* wrkr = pplan->add_instances();
    wrkr->set_instance_id(*iter);
    wrkr->set_stmgr_id(stmgr_id_);
    heron::proto::system::InstanceInfo* instance = wrkr->mutable_info();
    instance->set_task_id(global_index++);
    instance->set_component_index(instance_index);
    instance->set_component_name("exclaim1");
  }

  return pplan;
}

StMgr::~StMgr() {
  delete pplan_;
  delete state_mgr_;
}

void StMgr::HandleNewConnection(Connection*) {
  // There is nothing to be done here. Instead we wait
  // for the register
}

void StMgr::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  for (std::vector<Connection*>::iterator iter = spout_connections_.begin();
       iter != spout_connections_.end(); ++iter) {
    if (*iter == _conn) {
      spout_connections_.erase(iter);
      return;
    }
  }
  for (std::vector<Connection*>::iterator iter = bolt_connections_.begin();
       iter != bolt_connections_.end(); ++iter) {
    if (*iter == _conn) {
      bolt_connections_.erase(iter);
      return;
    }
  }
}

void StMgr::HandleRegisterInstanceRequest(REQID _reqid, Connection* _conn,
                                          proto::stmgr::RegisterInstanceRequest* _request) {
  if (spout_instances_.find(_request->topology_id()) != spout_instances_.end()) {
    spout_connections_.push_back(_conn);
  } else if (bolt_instances_.find(_request->topology_id()) != bolt_instances_.end()) {
    bolt_connections_.push_back(_conn);
  } else {
    LOG(ERROR) << "Unknown instance joined with us " << _request->topology_id() << std::endl;
    ::exit(1);
  }
  proto::stmgr::RegisterInstanceResponse response;
  proto::system::Status* st = response.mutable_status();
  st->set_status(proto::system::OK);
  response.mutable_pplan()->CopyFrom(*pplan_);
  SendResponse(_reqid, _conn, response);
  delete _request;
}

void StMgr::HandleTupleSetMessage(Connection* _conn, proto::stmgr::TupleMessage* _message) {
  bool is_spout = false;
  bool is_bolt = false;
  for (size_t i = 0; i < spout_connections_.size(); ++i) {
    if (spout_connections_[i] == _conn) {
      is_spout = true;
      break;
    }
  }
  for (size_t i = 0; i < bolt_connections_.size(); ++i) {
    if (bolt_connections_[i] == _conn) {
      is_bolt = true;
      break;
    }
  }

  if (is_spout) {
    if (bolt_connections_.size() > 0) {
      SendSpoutMessageToBolt(_message);
    } else {
      LOG(INFO) << "Throwing data away since the other party is not alive" << std::endl;
    }
  } else if (is_bolt) {
    if (spout_connections_.size() > 0) {
      spout_index_ = (spout_index_ + 1) % spout_connections_.size();
      SendMessage(spout_connections_[spout_index_], *_message);
      LOG(INFO) << "NO ACKING" << std::endl;
      ::exit(1);
    } else {
      LOG(INFO) << "Throwing data away since the other party is not alive" << std::endl;
    }
  }
  delete _message;
}

void StMgr::SendSpoutMessageToBolt(proto::stmgr::TupleMessage* _message) {
  std::map<Connection*, proto::stmgr::TupleMessage*> mymap;
  bolt_index_ = (bolt_index_ + 1) % bolt_connections_.size();
  Connection* to_be_sent_on = bolt_connections_[bolt_index_];
  proto::system::HeronDataTupleSet* d = _message->mutable_set()->mutable_data();
  for (sp_int32 i = 0; i < d->tuples_size(); ++i) {
    proto::stmgr::TupleMessage* m = NULL;
    if (mymap.find(to_be_sent_on) == mymap.end()) {
      m = new proto::stmgr::TupleMessage();
      m->mutable_set()->mutable_data()->mutable_stream()->CopyFrom(d->stream());
      mymap[to_be_sent_on] = m;
    } else {
      m = mymap[to_be_sent_on];
    }
    proto::system::HeronDataTuple* added_tuple;
    added_tuple = m->mutable_set()->mutable_data()->add_tuples();
    added_tuple->CopyFrom(d->tuples(i));
    sp_int64 tuple_key = lrand();
    added_tuple->set_key(tuple_key);
  }

  std::map<Connection*, proto::stmgr::TupleMessage*>::iterator iter;
  for (iter = mymap.begin(); iter != mymap.end(); ++iter) {
    SendMessage(iter->first, *(iter->second));
    delete iter->second;
  }
}
}
}  // end of namespace
