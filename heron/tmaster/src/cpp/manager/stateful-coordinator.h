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

#ifndef __TMASTER_STATEFUL_COORDINATOR_H_
#define __TMASTER_STATEFUL_COORDINATOR_H_

#include "network/network.h"
#include "proto/tmaster.pb.h"
#include "manager/tmaster.h"
#include "basics/basics.h"

namespace heron {
namespace tmaster {

class StatefulCoordinator {
 public:
  explicit StatefulCoordinator(std::chrono::high_resolution_clock::time_point _tmaster_start_time);
  virtual ~StatefulCoordinator();

  void DoCheckpoint(const StMgrMap& _stmgrs);

 private:
  sp_string GenerateCheckpointId();

  std::chrono::high_resolution_clock::time_point tmaster_start_time_;
};
}  // namespace tmaster
}  // namespace heron

#endif
