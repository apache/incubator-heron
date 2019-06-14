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

#ifndef __DUMMY_INSTANCE_SERVER_H
#define __DUMMY_INSTANCE_SERVER_H

#include <set>
#include <string>
#include <vector>

#include "proto/messages.h"
#include "util/neighbour-calculator.h"
#include "manager/instance-server.h"

class DummyInstanceServer : public heron::stmgr::InstanceServer {
 public:
  DummyInstanceServer(std::shared_ptr<EventLoop> _eventLoop, const NetworkOptions& _options,
                   const std::string& _stmgr,
                   heron::proto::system::PhysicalPlan* _pplan,
                   const std::vector<sp_string>& _expected_instances,
                   std::shared_ptr<heron::common::MetricsMgrSt> const& _metrics)
  : heron::stmgr::InstanceServer(_eventLoop, _options, _pplan->topology().name(),
                              _pplan->topology().id(), _stmgr,
                              _expected_instances, nullptr, _metrics,
                              std::make_shared<heron::stmgr::NeighbourCalculator>(), false),
    pplan_(_pplan), clear_called_(false), all_instances_connected_(false),
    my_stmgr_id_(_stmgr) {
  }

  virtual ~DummyInstanceServer() { }

  virtual void ClearCache() { clear_called_ = true; }
  bool ClearCalled() const { return clear_called_; }

  virtual void GetInstanceInfo(std::vector<heron::proto::system::Instance*>& _return) {
    for (int i = 0; i < pplan_->instances_size(); ++i) {
      if (pplan_->instances(i).stmgr_id() == my_stmgr_id_) {
        _return.push_back(pplan_->mutable_instances(i));
      }
    }
  }
  virtual heron::proto::system::Instance* GetInstanceInfo(int32_t _task_id) {
    for (int i = 0; i < pplan_->instances_size(); ++i) {
       if (pplan_->instances(i).info().task_id() == _task_id) {
         return pplan_->mutable_instances(i);
       }
    }
    return NULL;
  }

  virtual bool HaveAllInstancesConnectedToUs() const { return all_instances_connected_; }
  virtual void SetAllInstancesConnectedToUs(bool val) { all_instances_connected_ = val; }

  virtual bool SendRestoreInstanceStateRequest(sp_int32 _task_id,
      const heron::proto::ckptmgr::InstanceStateCheckpoint&) {
    restore_sent_.insert(_task_id);
    return true;
  }
  bool DidSendRestoreRequest(sp_int32 _task_id) {
    return restore_sent_.find(_task_id) != restore_sent_.end();
  }
  void ClearSendRestoreRequest(sp_int32 _task_id) {
    restore_sent_.erase(_task_id);
  }

 private:
  heron::proto::system::PhysicalPlan* pplan_;
  bool clear_called_;
  bool all_instances_connected_;
  std::set<int32_t> restore_sent_;
  std::string my_stmgr_id_;
};

#endif
