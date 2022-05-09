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

#include "manager/stateful-checkpointer.h"
#include <iostream>
#include <sstream>
#include <chrono>
#include <string>
#include "config/physical-plan-helper.h"
#include "manager/tmanager.h"
#include "manager/stmgrstate.h"
#include "errors/errors.h"

namespace heron {
namespace tmanager {

StatefulCheckpointer::StatefulCheckpointer(
  std::chrono::high_resolution_clock::time_point _tmanager_start_time)
  : tmanager_start_time_(_tmanager_start_time) {
  // do nothing
}

StatefulCheckpointer::~StatefulCheckpointer() { }

sp_string StatefulCheckpointer::GenerateCheckpointId() {
  // TODO(skukarni) Should we append any topology name/id stuff?
  std::ostringstream tag;
  tag << tmanager_start_time_.time_since_epoch().count()
      << "-" << time(NULL);
  return tag.str();
}

void StatefulCheckpointer::StartCheckpoint(const StMgrMap& _stmgrs) {
  // TODO(nlu) we should avoid checkpointer starvation problem when the interval is small
  // Generate the checkpoint id
  sp_string checkpoint_id = GenerateCheckpointId();

  // Send the checkpoint message to all active stmgrs
  LOG(INFO) << "Sending checkpoint message with id: " << checkpoint_id
            << " to all stmgrs";
  for (auto iter = _stmgrs.begin(); iter != _stmgrs.end(); ++iter) {
    proto::ckptmgr::StartStatefulCheckpoint request;
    request.set_checkpoint_id(checkpoint_id);
    iter->second->NewStatefulCheckpoint(request);
  }
}

void StatefulCheckpointer::RegisterNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  config::PhysicalPlanHelper::GetAllTasks(_pplan, all_tasks_);
}

bool StatefulCheckpointer::HandleInstanceStateStored(const std::string& _checkpoint_id,
                                          const proto::system::Instance& _instance) {
  LOG(INFO) << "Handling InstanceStateStored for checkpoint: " << _checkpoint_id
            << " and instance " << _instance.info().task_id();
  if (current_partial_checkpoint_.empty()) {
    LOG(INFO) << "Seeing the checkpoint id for the first time";
    partial_checkpoint_remaining_tasks_ = all_tasks_;
    current_partial_checkpoint_ = _checkpoint_id;
    partial_checkpoint_remaining_tasks_.erase(_instance.info().task_id());
  } else if (_checkpoint_id > current_partial_checkpoint_) {
    LOG(INFO) << "This new checkpoint id is newer than current partial one "
              << current_partial_checkpoint_;
    partial_checkpoint_remaining_tasks_ = all_tasks_;
    current_partial_checkpoint_ = _checkpoint_id;
    partial_checkpoint_remaining_tasks_.erase(_instance.info().task_id());
  } else if (_checkpoint_id == current_partial_checkpoint_) {
    LOG(INFO) << "This checkpoint id equals to the current partial one";
    partial_checkpoint_remaining_tasks_.erase(_instance.info().task_id());
  } else {
    LOG(INFO) << "This checkpoint id is older than partial one "
              << current_partial_checkpoint_;
  }

  if (partial_checkpoint_remaining_tasks_.empty()) {
    LOG(INFO) << "All task ids have their state stored for "
              << current_partial_checkpoint_;
    current_partial_checkpoint_ = "";
    return true;
  } else {
    return false;
  }
}
}  // namespace tmanager
}  // namespace heron
