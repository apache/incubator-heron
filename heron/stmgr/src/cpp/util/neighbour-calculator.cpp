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

#include "util/neighbour-calculator.h"
#include <functional>
#include <iostream>
#include <map>
#include <unordered_set>
#include "config/physical-plan-helper.h"

namespace heron {
namespace stmgr {

NeighbourCalculator::NeighbourCalculator() {
}

NeighbourCalculator::~NeighbourCalculator() {
}

void NeighbourCalculator::Reconstruct(const proto::system::PhysicalPlan& _pplan) {
  to_send_list_.clear();
  from_recv_list_.clear();

  // First deal with spouts
  for (sp_int32 i = 0; i < _pplan.topology().spouts_size(); ++i) {
    std::unordered_set<sp_int32> spout_tasks;
    config::PhysicalPlanHelper::GetComponentTasks(_pplan,
                                _pplan.topology().spouts(i).comp().name(),
                                spout_tasks);
    // Spouts don't have any upstreamers
    for (auto spout_task : spout_tasks) {
      from_recv_list_[spout_task] = std::unordered_set<sp_int32>();
    }
    // The to_send list of spouts will be updated in the bolts section
  }

  // Now deal with bolts
  for (sp_int32 i = 0; i < _pplan.topology().bolts_size(); ++i) {
    std::unordered_set<sp_int32> bolt_tasks;
    config::PhysicalPlanHelper::GetComponentTasks(_pplan,
                                _pplan.topology().bolts(i).comp().name(),
                                bolt_tasks);

    for (auto input_stream : _pplan.topology().bolts(i).inputs()) {
      sp_string producer_comp = input_stream.stream().component_name();
      std::unordered_set<sp_int32> tasks;
      config::PhysicalPlanHelper::GetComponentTasks(_pplan, producer_comp, tasks);
      // each of the bolt task needs to recv from all the producer tasks
      for (auto bolt_task : bolt_tasks) {
        add(from_recv_list_, bolt_task, tasks);
      }
      // each of the producer tasks needs to send to all bolt tasks
      for (auto task : tasks) {
        add(to_send_list_, task, bolt_tasks);
      }
    }
  }

  LOG(INFO) << "Reconstructed Upstream/Downstream Checkpoint Dependencies";
  LOG(INFO) << *this;
}

void NeighbourCalculator::add(std::map<sp_int32, std::unordered_set<sp_int32>>& _map,
                         sp_int32 _key, std::unordered_set<sp_int32>& _values) {
  if (_map.find(_key) == _map.end()) {
    _map[_key] = _values;
  } else {
    _map[_key].insert(_values.begin(), _values.end());
  }
}

std::unordered_set<sp_int32> NeighbourCalculator::get_downstreamers(sp_int32 _task_id) {
  if (to_send_list_.find(_task_id) == to_send_list_.end()) {
    return std::unordered_set<sp_int32>();
  } else {
    return to_send_list_[_task_id];
  }
}

std::unordered_set<sp_int32> NeighbourCalculator::get_upstreamers(sp_int32 _task_id) {
  if (from_recv_list_.find(_task_id) == from_recv_list_.end()) {
    return std::unordered_set<sp_int32>();
  } else {
    return from_recv_list_[_task_id];
  }
}

std::ostream& operator<<(std::ostream& _os, const NeighbourCalculator& _obj) {
  for (auto kv : _obj.from_recv_list_) {
    _os << "Upstream Dependencies for task_id " << kv.first << ": ";
    for (auto upstream_task : kv.second) {
      _os << " " << upstream_task;
    }
    _os << "\n";
  }
  for (auto kv : _obj.to_send_list_) {
    _os << "Downstream Dependencies for task_id " << kv.first << ": ";
    for (auto downstream_task : kv.second) {
      _os << " " << downstream_task;
    }
    _os << "\n";
  }
  return _os;
}
}  // namespace stmgr
}  // namespace heron
