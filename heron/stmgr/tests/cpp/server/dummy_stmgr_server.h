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

#ifndef __DUMMY_STMGR_SERVER_H
#define __DUMMY_STMGR_SERVER_H

#include <set>
#include <string>
#include <vector>

#include "proto/messages.h"
#include "util/neighbour-calculator.h"
#include "manager/stmgr-server.h"

class DummyStMgrServer : public heron::stmgr::StMgrServer {
 public:
  DummyStMgrServer(EventLoop* _eventLoop, const NetworkOptions& _options,
                   const std::string& _stmgr,
                   heron::proto::system::PhysicalPlan* _pplan,
                   const std::vector<sp_string>& _expected_instances,
                   heron::common::MetricsMgrSt* _metrics)
  : heron::stmgr::StMgrServer(_eventLoop, _options, _pplan->topology().name(),
                              _pplan->topology().id(), _stmgr,
                              _expected_instances, NULL, _metrics,
                              new heron::stmgr::NeighbourCalculator()),
    pplan_(_pplan), clear_called_(false), all_instances_connected_(false),
    my_stmgr_id_(_stmgr) {
  }

  virtual ~DummyStMgrServer() { }

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
