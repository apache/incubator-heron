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

#if !defined(CHECKPOINT_H)
#define CHECKPOINT_H

#include <string>
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace state {

class Checkpoint {
 public:
  Checkpoint(const std::string& topology,
             ::heron::proto::ckptmgr::SaveStateCheckpoint* _checkpoint);

  Checkpoint(const std::string& topology, const std::string& ckptid,
             const std::string& component, const std::string& instance)
      : topology_(topology), ckptid_(ckptid), component_(component), instance_(instance) {}

  virtual ~Checkpoint() {}

  // get the topology name
  std::string getTopology() const { return topology_; }

  // get the checkpoint id
  std::string getCkptId() const { return ckptid_; }

  // get the component id
  std::string getComponent() const { return component_; }

  // get the instance id
  std::string getInstance() const { return instance_; }

  // get the checkpoint bytes
  ::heron::proto::ckptmgr::SaveStateCheckpoint* bytes() const { return savebytes_; }

  // get the total number of bytes to be saved
  sp_int32 nbytes() const { return nbytes_; }

 private:
  std::string  topology_;    // topology name
  std::string  ckptid_;      // checkpoint id
  std::string  component_;   // component id
  std::string  instance_;    // instance id
  sp_int32     nbytes_;      // number of bytes
  ::heron::proto::ckptmgr::SaveStateCheckpoint*  savebytes_;
};

}  // namespace state
}  // namespace heron

#endif  // ckpt.h
