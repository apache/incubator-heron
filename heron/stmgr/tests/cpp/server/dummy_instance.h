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

#ifndef __DUMMY_INSTANCE_H
#define __DUMMY_INSTANCE_H

#include "proto/messages.h"
#include "network/network_error.h"

class DummyInstance : public Client {
 public:
  DummyInstance(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options,
                const sp_string& _topology_name, const sp_string& _topology_id,
                const sp_string& _instance_id, const sp_string& _component_name, sp_int32 _task_id,
                sp_int32 _component_index, const sp_string& _stmgr_id);

  virtual ~DummyInstance();

  sp_int32 get_task_id() const { return task_id_; }
  heron::proto::system::StatusCode GetRegisterResponseStatus();

 protected:
  void Retry() { Start(); }

  // Handle incoming message
  virtual void HandleInstanceResponse(
                            void* ctx,
                            pool_unique_ptr<heron::proto::stmgr::RegisterInstanceResponse> _message,
                            NetworkErrorCode status);
  // Handle incoming tuples
  virtual void HandleTupleMessage(pool_unique_ptr<heron::proto::system::HeronTupleSet2> _message);
  // Handle the instance assignment message
  virtual void HandleNewInstanceAssignmentMsg(
                              pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage>);

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string instance_id_;
  sp_string component_name_;
  sp_int32 task_id_;
  sp_int32 component_index_;
  sp_string stmgr_id_;

 private:
  // Handle incoming connections
  virtual void HandleConnect(NetworkErrorCode _status);
  // Handle connection close
  virtual void HandleClose(NetworkErrorCode _status);
  // Send worker request
  void CreateAndSendInstanceRequest();

  VCallback<> retry_cb_;
  heron::proto::system::PhysicalPlan* recvd_stmgr_pplan_;
  heron::proto::system::StatusCode register_response_status;
};

class DummySpoutInstance : public DummyInstance {
 public:
  DummySpoutInstance(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options,
                     const sp_string& _topology_name, const sp_string& _topology_id,
                     const sp_string& _instance_id, const sp_string& _component_name,
                     sp_int32 _task_id, sp_int32 _component_index, const sp_string& _stmgr_id,
                     const sp_string& _stream_id, sp_int32 _max_msgs_to_send,
                     bool _do_custom_grouping);

 protected:
  // Handle incoming message
  virtual void HandleNewInstanceAssignmentMsg(
          pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage> _msg);
  void CreateAndSendTupleMessages();
  virtual void StartBackPressureConnectionCb(Connection* connection) {
    under_backpressure_ = true;
  }
  virtual void StopBackPressureConnectionCb(Connection* _connection) {
    under_backpressure_ = false;
  }

 private:
  sp_string stream_id_;
  sp_int32 max_msgs_to_send_;
  sp_int32 total_msgs_sent_;
  sp_int32 batch_size_;
  bool do_custom_grouping_;
  bool under_backpressure_;
  // only valid when the above is true
  sp_int32 custom_grouping_dest_task_;
};

class DummyBoltInstance : public DummyInstance {
 public:
  DummyBoltInstance(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options,
                    const sp_string& _topology_name, const sp_string& _topology_id,
                    const sp_string& _instance_id, const sp_string& _component_name,
                    sp_int32 _task_id, sp_int32 _component_index, const sp_string& _stmgr_id,
                    sp_int32 _expected_msgs_to_recv);

  sp_int32 MsgsRecvd() { return msgs_recvd_; }

 protected:
  // Handle incoming message
  // Handle incoming tuples
  virtual void HandleTupleMessage(pool_unique_ptr<heron::proto::system::HeronTupleSet2> _message);
  virtual void HandleNewInstanceAssignmentMsg(
          pool_unique_ptr<heron::proto::stmgr::NewInstanceAssignmentMessage> _msg);

 private:
  sp_int32 expected_msgs_to_recv_;
  sp_int32 msgs_recvd_;
};

#endif
