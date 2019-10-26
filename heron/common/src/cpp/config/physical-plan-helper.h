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

/////////////////////////////////////////////////////////////
//
// physical-plan-helper.h
//
// This file defines some helper methods to get some
// variables from the physical plan
//
/////////////////////////////////////////////////////////////
#ifndef SVCS_COMMON_CONFIG_PHYSICAL_PLAN_HELPER_H_
#define SVCS_COMMON_CONFIG_PHYSICAL_PLAN_HELPER_H_

#include <list>
#include <map>
#include <string>
#include <unordered_set>
#include "basics/basics.h"
#include "proto/messages.h"

namespace heron {
namespace config {

class PhysicalPlanHelper {
 public:
  class TaskData {
   public:
    sp_int32 task_id_;
    sp_string component_name_;
  };
  // Returns the map of <worker_id, task_id> that belong
  // to _stmgr
  static void GetLocalTasks(const proto::system::PhysicalPlan& _pplan, const sp_string& _stmgr,
                            std::map<sp_string, TaskData>& _return);

  // Returns the map of <worker_id, task_id> of the spouts
  // that belong to the _stmgr
  static void GetLocalSpouts(const proto::system::PhysicalPlan& _pplan, const sp_string& _stmgr,
                             std::unordered_set<sp_int32>& _return);

  // Return the list of all task_ids that are serviced by the stmgr
  static void GetTasks(const proto::system::PhysicalPlan& _pplan, const sp_string& _stmgr,
                       std::unordered_set<sp_int32>& _return);

  // Return the list of all task_ids that belong to this component
  static void GetComponentTasks(const proto::system::PhysicalPlan& _pplan,
                                const sp_string& _component,
                                std::unordered_set<sp_int32>& _return);

  // Return the set of all task_ids across the entire topology
  static void GetAllTasks(const proto::system::PhysicalPlan& _pplan,
                          std::unordered_set<sp_int32>& _return);

  static void LogPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  // Returns the component name for the specified _task_id
  // If the _task_id is not part of the _pplan, return empty string
  static const std::string& GetComponentName(const proto::system::PhysicalPlan& _pplan,
                                             int _task_id);

  // For a particular _component, returns all the task_ids
  static void GetComponentTaskIds(const proto::system::PhysicalPlan& _pplan,
                                  const std::string& _component, std::list<int>& _retval);

  // Return a mapping from task id -> component name
  static void GetTaskIdToComponentName(const proto::system::PhysicalPlan& _pplan,
                                       std::map<int, std::string>& retval);
};
}  // namespace config
}  // namespace heron

#endif
