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

#include <map>
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

  static void LogPhysicalPlan(const proto::system::PhysicalPlan& _pplan);
};
}  // namespace config
}  // namespace heron

#endif
