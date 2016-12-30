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

#if !defined(FS_CKPT_H)
#define FS_CKPT_H

#include <string>
#include "common/ckpt.h"

namespace heron {
namespace state {

class FSCheckpoint : public Checkpoint {
 public:
  FSCheckpoint(const std::string& topology, const std::string& ckptid,
               const std::string& component, const std::string& instance,
               const std::string& base_dir)
      : Checkpoint(topology, ckptid, component, instance), base_dir_(base_dir) {}

  virtual ~FSCheckpoint() {}

  // get the name of the base directory
  std::string getBaseDir() { return base_dir_; }

  // get the checkpoint file name
  std::string getFile();

 private:
  std::string  base_dir_;
};

}  // namespace state
}  // namespace heron

#endif  // fsckpt.h
