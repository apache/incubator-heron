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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_HELPER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_HELPER_H_

#include <ostream>
#include <map>
#include <set>
#include <vector>
#include <typeinfo>   // operator typeid
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "grouping/shuffle-grouping.h"

namespace heron {
namespace stmgr {

class StatefulHelper {
 public:
  StatefulHelper();
  void Reconstruct(const proto::system::PhysicalPlan& _pplan);
  virtual ~StatefulHelper();

  std::set<sp_int32> get_downstreamers(sp_int32 _task_id);
  std::set<sp_int32> get_upstreamers(sp_int32 _task_id);

 private:
  void add(std::map<sp_int32, std::set<sp_int32>>& _set,
           sp_int32 _key, std::set<sp_int32>& _values);

  // This is a map from task_id to all the task_ids
  // to which this task should send the checkpoint marker message
  std::map<sp_int32, std::set<sp_int32>> to_send_list_;
  // This is a map from task_id to all the task_ids
  // from which this task should get the checkpoint marker message
  // before it can save its state
  std::map<sp_int32, std::set<sp_int32>> from_recv_list_;

  friend std::ostream& operator<<(std::ostream& os, const StatefulHelper& obj);
};

std::ostream& operator<<(std::ostream& os, const StatefulHelper& obj);

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_HELPER_H_
