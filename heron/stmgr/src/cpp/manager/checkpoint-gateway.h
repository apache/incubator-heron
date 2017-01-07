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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_

#include <list>
#include <map>
#include <set>
#include <vector>
#include <typeinfo>   // operator typeid
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace stmgr {

typedef std::tuple<

class StatefulHelper;

class CheckpointGateway {
 public:
  CheckpointGateway(sp_uint32 _drain_threshold, StatefulHelper* _stateful_helper);
  virtual ~CheckpointGateway();

 private:
  // The maximum buffering that we can do before we discard the marker
  sp_uint32 drain_threshold_;
  StatefulHelper* stateful_helper_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_
