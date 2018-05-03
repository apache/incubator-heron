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

#include "config/physical-plan-helper.h"
#include <map>
#include <unordered_set>
#include "basics/basics.h"
#include "errors/errors.h"
#include "proto/messages.h"
#include "network/network.h"
#include "threads/threads.h"


namespace heron {
namespace config {

void PhysicalPlanHelper::GetLocalTasks(const proto::system::PhysicalPlan& _pplan,
                                       const sp_string& _stmgr,
                                       std::map<sp_string, TaskData>& _return) {
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.stmgr_id() == _stmgr) {
      TaskData tdata;
      tdata.task_id_ = instance.info().task_id();
      tdata.component_name_ = instance.info().component_name();
      _return[instance.instance_id()] = tdata;
    }
  }
  return;
}

void PhysicalPlanHelper::GetLocalSpouts(const proto::system::PhysicalPlan& _pplan,
                                        const sp_string& _stmgr,
                                        std::unordered_set<sp_int32>& _return) {
  std::unordered_set<sp_string> spouts;
  for (sp_int32 i = 0; i < _pplan.topology().spouts_size(); ++i) {
    spouts.insert(_pplan.topology().spouts(i).comp().name());
  }
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.stmgr_id() == _stmgr &&
        spouts.find(instance.info().component_name()) != spouts.end()) {
      _return.insert(instance.info().task_id());
    }
  }
  return;
}

const std::string& PhysicalPlanHelper::GetComponentName(const proto::system::PhysicalPlan& _pplan,
                                                        int _task_id) {
  for (auto instance : _pplan.instances()) {
    if (instance.info().task_id() == _task_id) {
      return instance.info().component_name();
    }
  }
  return EMPTY_STRING;
}

void PhysicalPlanHelper::GetComponentTaskIds(const proto::system::PhysicalPlan& _pplan,
                                             const std::string& _component,
                                             std::list<int>& _retval) {
  for (auto instance : _pplan.instances()) {
    if (instance.info().component_name() == _component) {
      _retval.push_back(instance.info().task_id());
    }
  }
}

void PhysicalPlanHelper::GetTaskIdToComponentName(const proto::system::PhysicalPlan& _pplan,
                                                  std::map<int, std::string>& retval) {
  for (auto instance : _pplan.instances()) {
    retval[instance.info().task_id()] = instance.info().component_name();
  }
}

void PhysicalPlanHelper::GetTasks(const proto::system::PhysicalPlan& _pplan,
                                  const sp_string& _stmgr,
                                  std::unordered_set<sp_int32>& _return) {
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.stmgr_id() == _stmgr) {
      _return.insert(instance.info().task_id());
    }
  }
  return;
}

void PhysicalPlanHelper::GetAllTasks(const proto::system::PhysicalPlan& _pplan,
                                     std::unordered_set<sp_int32>& _return) {
  for (auto stmgr : _pplan.stmgrs()) {
    GetTasks(_pplan, stmgr.id(), _return);
  }
  return;
}

void PhysicalPlanHelper::GetComponentTasks(const proto::system::PhysicalPlan& _pplan,
                                           const sp_string& _component,
                                           std::unordered_set<sp_int32>& _return) {
  for (int i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    if (instance.info().component_name() == _component) {
      _return.insert(instance.info().task_id());
    }
  }
}

void PhysicalPlanHelper::LogPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  LOG(INFO) << "Printing Physical Plan" << std::endl;
  LOG(INFO) << "Topology Name: " << _pplan.topology().name();
  LOG(INFO) << "Topology Id: " << _pplan.topology().id();
  LOG(INFO) << "Number of Stmgrs: " << _pplan.stmgrs_size();
  for (sp_int32 i = 0; i < _pplan.stmgrs_size(); ++i) {
    const proto::system::StMgr& stmgr = _pplan.stmgrs(i);
    LOG(INFO) << "\tStMgr id: " << stmgr.id();
    LOG(INFO) << "\tStMgr host_name: " << stmgr.host_name();
    LOG(INFO) << "\tStMgr data_port: " << stmgr.data_port();
    LOG(INFO) << "\tStMgr local_endpoint: " << stmgr.local_endpoint();
    LOG(INFO) << "\tStMgr cwd: " << stmgr.cwd();
    LOG(INFO) << "\tStMgr PID: " << stmgr.pid();
  }
  LOG(INFO) << "Total number of Instances: " << _pplan.instances_size();
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    const proto::system::Instance& instance = _pplan.instances(i);
    LOG(INFO) << "\tInstance id: " << instance.instance_id();
    LOG(INFO) << "\tMy Stmgr id: " << instance.stmgr_id();
    LOG(INFO) << "\tMy task_id: " << instance.info().task_id();
    LOG(INFO) << "\tMy component_index: " << instance.info().component_index();
    LOG(INFO) << "\tMy component name: " << instance.info().component_name();
  }

  LOG(INFO) << "Topology State: " << _pplan.topology().state();
}
}  // namespace config
}  // namespace heron
