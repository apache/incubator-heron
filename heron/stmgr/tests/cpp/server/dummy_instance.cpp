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

#include <stdio.h>
#include <iostream>
#include <limits>
#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "server/dummy_instance.h"

DummyInstance::DummyInstance(std::shared_ptr<EventLoopImpl> eventLoop,
                             const NetworkOptions& _options,
                             const sp_string& _topology_name, const sp_string& _topology_id,
                             const sp_string& _instance_id, const sp_string& _component_name,
                             sp_int32 _task_id, sp_int32 _component_index,
                             const sp_string& _stmgr_id)
    : Client(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      instance_id_(_instance_id),
      component_name_(_component_name),
      task_id_(_task_id),
      component_index_(_component_index),
      stmgr_id_(_stmgr_id),
      recvd_stmgr_pplan_(NULL),
      register_response_status(heron::proto::system::STMGR_DIDNT_REGISTER) {
  InstallResponseHandler(make_unique<heron::proto::stmgr::RegisterInstanceRequest>(),
                         &DummyInstance::HandleInstanceResponse);
  InstallMessageHandler(&DummyInstance::HandleTupleMessage);
  InstallMessageHandler(&DummyInstance::HandleNewInstanceAssignmentMsg);

  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this]() { this->Retry(); };
}

DummyInstance::~DummyInstance() {
  Stop();
  if (recvd_stmgr_pplan_) delete recvd_stmgr_pplan_;
}

void DummyInstance::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    CreateAndSendInstanceRequest();
  } else {
    // Retry after some time
    AddTimer(retry_cb_, 100);
  }
}

void DummyInstance::HandleClose(NetworkErrorCode) {
  AddTimer(retry_cb_, 100);
}

heron::proto::system::StatusCode DummyInstance::GetRegisterResponseStatus() {
  return register_response_status;
}

void DummyInstance::HandleInstanceResponse(
                            void*,
                            pool_unique_ptr<heron::proto::stmgr::RegisterInstanceResponse> _message,
                            NetworkErrorCode status) {
  CHECK_EQ(status, OK);
  if (_message->has_pplan()) {
    if (recvd_stmgr_pplan_) {
      delete recvd_stmgr_pplan_;
    }

    recvd_stmgr_pplan_ = new heron::proto::system::PhysicalPlan();
    recvd_stmgr_pplan_->CopyFrom(_message->pplan());
  }

  register_response_status = _message->status().status();
}

void DummyInstance::HandleTupleMessage(pool_unique_ptr<heron::proto::system::HeronTupleSet2>) {}

void DummyInstance::HandleNewInstanceAssignmentMsg(
        pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage>) {}

void DummyInstance::CreateAndSendInstanceRequest() {
  auto request = make_unique<heron::proto::stmgr::RegisterInstanceRequest>();
  heron::proto::system::Instance* instance = request->mutable_instance();
  instance->set_instance_id(instance_id_);
  instance->set_stmgr_id(stmgr_id_);
  instance->mutable_info()->set_task_id(task_id_);
  instance->mutable_info()->set_component_index(component_index_);
  instance->mutable_info()->set_component_name(component_name_);
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  SendRequest(std::move(request), nullptr);
  return;
}

//////////////////////////////////////// DummySpoutInstance ////////////////////////////////////
DummySpoutInstance::DummySpoutInstance(std::shared_ptr<EventLoopImpl> eventLoop,
                                       const NetworkOptions& _options,
                                       const sp_string& _topology_name,
                                       const sp_string& _topology_id, const sp_string& _instance_id,
                                       const sp_string& _component_name, sp_int32 _task_id,
                                       sp_int32 _component_index, const sp_string& _stmgr_id,
                                       const sp_string& stream_id, sp_int32 max_msgs_to_send,
                                       bool _do_custom_grouping)
    : DummyInstance(eventLoop, _options, _topology_name, _topology_id, _instance_id,
                    _component_name, _task_id, _component_index, _stmgr_id),
      stream_id_(stream_id),
      max_msgs_to_send_(max_msgs_to_send),
      total_msgs_sent_(0),
      batch_size_(1000),
      do_custom_grouping_(_do_custom_grouping),
      under_backpressure_(false) {}

void DummySpoutInstance::HandleNewInstanceAssignmentMsg(
        pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage> _msg) {
  const heron::proto::system::PhysicalPlan pplan = _msg->pplan();

  DummyInstance::HandleNewInstanceAssignmentMsg(std::move(_msg));

  custom_grouping_dest_task_ = std::numeric_limits<sp_int32>::max() - 1;
  if (do_custom_grouping_) {
    for (sp_int32 i = 0; i < pplan.instances_size(); ++i) {
      if (pplan.instances(i).info().component_name() != component_name_ &&
          pplan.instances(i).info().task_id() < custom_grouping_dest_task_) {
        custom_grouping_dest_task_ = pplan.instances(i).info().task_id();
      }
    }
  }

  CreateAndSendTupleMessages();
}

void DummySpoutInstance::CreateAndSendTupleMessages() {
  if (!under_backpressure_) {
    for (int i = 0; (i < batch_size_) && (total_msgs_sent_ < max_msgs_to_send_);
         ++total_msgs_sent_, ++i) {
      heron::proto::system::HeronTupleSet tuple_set;
      heron::proto::system::HeronDataTupleSet* data_set = tuple_set.mutable_data();
      heron::proto::api::StreamId* tstream = data_set->mutable_stream();
      tstream->set_id(stream_id_);
      tstream->set_component_name(component_name_);
      heron::proto::system::HeronDataTuple* tuple = data_set->add_tuples();
      tuple->set_key(0);
      // Add lots of data
      for (size_t i = 0; i < 500; ++i) *(tuple->add_values()) = "dummy data";

      // Add custom grouping if need be
      if (do_custom_grouping_) {
        tuple->add_dest_task_ids(custom_grouping_dest_task_);
      }
      SendMessage(tuple_set);
    }
  }
  if (total_msgs_sent_ != max_msgs_to_send_) {
    AddTimer([this]() { this->CreateAndSendTupleMessages(); }, 1000);
  }
}

//////////////////////////////////////// DummyBoltInstance ////////////////////////////////////
DummyBoltInstance::DummyBoltInstance(std::shared_ptr<EventLoopImpl> eventLoop,
                                     const NetworkOptions& _options,
                                     const sp_string& _topology_name, const sp_string& _topology_id,
                                     const sp_string& _instance_id,
                                     const sp_string& _component_name, sp_int32 _task_id,
                                     sp_int32 _component_index, const sp_string& _stmgr_id,
                                     sp_int32 _expected_msgs_to_recv)
    : DummyInstance(eventLoop, _options, _topology_name, _topology_id, _instance_id,
                    _component_name, _task_id, _component_index, _stmgr_id),
      expected_msgs_to_recv_(_expected_msgs_to_recv),
      msgs_recvd_(0) {}

void DummyBoltInstance::HandleTupleMessage(
        pool_unique_ptr<heron::proto::system::HeronTupleSet2> msg) {
  if (msg->has_data()) msgs_recvd_ += msg->mutable_data()->tuples_size();
  if (msgs_recvd_ >= expected_msgs_to_recv_) getEventLoop()->loopExit();
}

void DummyBoltInstance::HandleNewInstanceAssignmentMsg(
        pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage> _msg) {
  DummyInstance::HandleNewInstanceAssignmentMsg(std::move(_msg));
  if (expected_msgs_to_recv_ == 0) {
    getEventLoop()->loopExit();
  }
}
