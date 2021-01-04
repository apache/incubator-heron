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
#include <set>
#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "server/dummy_ckptmgr_client.h"

DummyCkptMgrClient::DummyCkptMgrClient(std::shared_ptr<EventLoop> _eventLoop,
                                       const NetworkOptions& _options,
                                       const std::string& _stmgr,
                                       heron::proto::system::PhysicalPlan* _pplan)
  : heron::stmgr::CkptMgrClient(_eventLoop, _options, _pplan->topology().name(),
                               _pplan->topology().id(), "ckptmgr", _stmgr,
                               std::bind(&DummyCkptMgrClient::DummySaveWatcher, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2),
                               std::bind(&DummyCkptMgrClient::DummyGetWatcher, this,
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3,
                                         std::placeholders::_4),
                               std::bind(&DummyCkptMgrClient::DummyRegisterWatcher, this)) {
}

DummyCkptMgrClient::~DummyCkptMgrClient() {
}

void DummyCkptMgrClient::SaveInstanceState(unique_ptr<heron::proto::ckptmgr::SaveInstanceStateRequest> _req) {
  const std::string& ckpt_id = _req->checkpoint().checkpoint_id();
  if (saves_.find(ckpt_id) == saves_.end()) {
    saves_[ckpt_id] = std::set<int32_t>();
  }
  saves_[ckpt_id].insert(_req->instance().info().task_id());
  _req.reset(nullptr);
}

void DummyCkptMgrClient::GetInstanceState(const heron::proto::system::Instance& _instance,
                                          const std::string& _ckpt_id) {
  if (gets_.find(_ckpt_id) == gets_.end()) {
    gets_[_ckpt_id] = std::set<int32_t>();
  }
  gets_[_ckpt_id].insert(_instance.info().task_id());
}

bool DummyCkptMgrClient::SaveCalled(const std::string& _ckpt_id, int32_t _task_id) const {
  auto iter = saves_.find(_ckpt_id);
  if (iter == saves_.end()) {
    return false;
  }
  return iter->second.find(_task_id) != iter->second.end();
}

bool DummyCkptMgrClient::GetCalled(const std::string& _ckpt_id, int32_t _task_id) const {
  auto iter = gets_.find(_ckpt_id);
  if (iter == gets_.end()) {
    return false;
  }
  return iter->second.find(_task_id) != iter->second.end();
}

void DummyCkptMgrClient::ClearGetCalled(const std::string& _ckpt_id, int32_t _task_id) {
  auto iter = gets_.find(_ckpt_id);
  if (iter == gets_.end()) {
    LOG(FATAL) << "No such ckpt found";
  }
  iter->second.erase(_task_id);
}

void DummyCkptMgrClient::DummySaveWatcher(const heron::proto::system::Instance&,
                                          const std::string&) {
}

void DummyCkptMgrClient::DummyGetWatcher(
    heron::proto::system::StatusCode,
    int32_t,
    const std::string&,
    const heron::proto::ckptmgr::InstanceStateCheckpoint&) {
}

void DummyCkptMgrClient::DummyRegisterWatcher() {
}
