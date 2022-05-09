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

#include "statemgr/heron-statemgr.h"
#include <iostream>
#include <string>
#include <vector>
#include "statemgr/heron-localfilestatemgr.h"
#include "statemgr/heron-zkstatemgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

shared_ptr<HeronStateMgr> HeronStateMgr::MakeStateMgr(
    const std::string& _zk_hostport,
    const std::string& _topleveldir,
    shared_ptr<EventLoop> eventLoop,
    bool exitOnSessionExpiry) {

  if (_zk_hostport.empty()) {
    return std::make_shared<HeronLocalFileStateMgr>(_topleveldir, eventLoop);
  } else {
    return std::make_shared<HeronZKStateMgr>(_zk_hostport, _topleveldir, eventLoop,
            exitOnSessionExpiry);
  }
}

HeronStateMgr::HeronStateMgr(const std::string& _topleveldir) {
  topleveldir_ = _topleveldir;
  // remove trailing '/'
  if (topleveldir_[topleveldir_.size() - 1] == '/') {
    topleveldir_ = std::string(topleveldir_, 0, topleveldir_.size() - 1);
  }
}

HeronStateMgr::~HeronStateMgr() {}

void HeronStateMgr::ListExecutionState(const std::vector<sp_string>& _topologies,
                                       std::vector<proto::system::ExecutionState*>* _return,
                                       VCallback<proto::system::StatusCode> cb) {
  for (sp_uint32 i = 0; i < _topologies.size(); ++i) {
    proto::system::ExecutionState* e = new proto::system::ExecutionState();
    auto size = _topologies.size();
    auto wCb = [this, _return, size, e, cb](proto::system::StatusCode status) {
      this->ListExecutionStateDone(_return, size, e, std::move(cb), status);
    };

    GetExecutionState(_topologies[i], e, std::move(wCb));
  }
}

void HeronStateMgr::ListExecutionStateDone(std::vector<proto::system::ExecutionState*>* _return,
                                           size_t _required_size, proto::system::ExecutionState* _s,
                                           VCallback<proto::system::StatusCode> cb,
                                           proto::system::StatusCode _status) {
  if (_status == proto::system::OK) {
    _return->push_back(_s);
  } else {
    _return->push_back(NULL);
    delete _s;
  }
  if (_return->size() == _required_size) {
    proto::system::StatusCode status = proto::system::OK;
    for (size_t i = 0; i < _required_size; ++i) {
      if ((*_return)[i] == NULL) {
        status = proto::system::NOTOK;
        // cleanup all entries
        for (size_t j = 0; j < _required_size; ++j) {
          delete (*_return)[j];
        }
        break;
      }
    }
    cb(status);
  } else {
    // A different callback will make us enter the earlier path
    return;
  }
}

std::string HeronStateMgr::GetTManagerLocationDir() { return topleveldir_ + "/tmanagers"; }
std::string HeronStateMgr::GetMetricsCacheLocationDir() { return topleveldir_ + "/metricscaches"; }

std::string HeronStateMgr::GetTopologyDir() { return topleveldir_ + "/topologies"; }

std::string HeronStateMgr::GetPhysicalPlanDir() { return topleveldir_ + "/pplans"; }

std::string HeronStateMgr::GetPackingPlanDir() { return topleveldir_ + "/packingplans"; }

std::string HeronStateMgr::GetExecutionStateDir() { return topleveldir_ + "/executionstate"; }

std::string HeronStateMgr::GetStatefulCheckpointsDir() {
  return topleveldir_ + "/statefulcheckpoints";
}

std::string HeronStateMgr::GetTManagerLocationPath(const std::string& _topname) {
  return GetTManagerLocationDir() + "/" + _topname;
}
std::string HeronStateMgr::GetMetricsCacheLocationPath(const std::string& _topname) {
  return GetMetricsCacheLocationDir() + "/" + _topname;
}

std::string HeronStateMgr::GetTopologyPath(const std::string& _topname) {
  return GetTopologyDir() + "/" + _topname;
}

std::string HeronStateMgr::GetPhysicalPlanPath(const std::string& _topname) {
  return GetPhysicalPlanDir() + "/" + _topname;
}

std::string HeronStateMgr::GetPackingPlanPath(const std::string& _topname) {
  return GetPackingPlanDir() + "/" + _topname;
}

std::string HeronStateMgr::GetExecutionStatePath(const std::string& _topname) {
  return GetExecutionStateDir() + "/" + _topname;
}

std::string HeronStateMgr::GetStatefulCheckpointsPath(const std::string& _topname) {
  return GetStatefulCheckpointsDir() + "/" + _topname;
}
}  // namespace common
}  // namespace heron
