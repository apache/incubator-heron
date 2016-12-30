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

#if !defined(CKPT_H)
#define CKPT_H

#include <string>

namespace heron {
namespace state {

class Checkpoint {
 public:
  Checkpoint(const std::string& topology, const std::string& ckptid,
             const std::string& component, const std::string& instance)
      : topology_(topology), ckptid_(ckptid), component_(component), instance_(instance) {}

  virtual ~Checkpoint() {}

  // get the topology name
  std::string getTopology() { return topology_; }

  // get the checkpoint id
  std::string getCkptId() { return ckptid_; }

  // get the component id
  std::string getComponent() { return component_; }

  // get the instance id
  std::string getInstance() { return instance_; }

  // store the checkpoint
  virtual void store(void* bytes) = 0;

  // retrieve the checkpoint
  virtual void retrieve(void* bytes) = 0;

 private:
  std::string      topology_;    // topology name
  std::string      ckptid_;      // checkpoint id
  std::string      component_;   // component id
  std::string      instance_;    // instance id
};

}  // namespace state
}  // namespace heron

#endif  // ckpt.h
