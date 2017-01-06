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

#include "common/checkpoint.h"
#include <string>
#include "proto/messages.h"

namespace heron {
namespace ckptmgr {

Checkpoint::Checkpoint(const std::string& _topology,
                       ::heron::proto::ckptmgr::SaveStateCheckpoint* _checkpoint) {
  topology_ = _topology;
  ckptid_ = _checkpoint->checkpoint().checkpoint_id();
  component_ = _checkpoint->instance().info().component_name();
  instance_ = _checkpoint->instance().instance_id();
  savebytes_ = _checkpoint;
  nbytes_ = _checkpoint->ByteSize();
}

Checkpoint::Checkpoint(const std::string& _topology,
                       ::heron::proto::ckptmgr::RestoreStateCheckpoint* _checkpoint) {
  topology_ = _topology;
  ckptid_ = _checkpoint->checkpoint_id();
  component_ = _checkpoint->instance().info().component_name();
  instance_ = _checkpoint->instance().instance_id();
  savebytes_ = nullptr;
  nbytes_ = 0;
}

}  // namespace ckptmgr
}  // namespace heron
