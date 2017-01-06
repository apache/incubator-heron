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

#if !defined(LOCAL_FILE_SYSTEM_H)
#define LOCAL_FILE_SYSTEM_H

#include <unistd.h>
#include <string>

#include "common/checkpoint.h"
#include "common/storage.h"

namespace heron {
namespace state {

class LFS : public Storage {
 public:
  // constructor
  explicit LFS(const std::string& _base_dir) : base_dir_(_base_dir) {}

  // destructor
  virtual ~LFS() {}

  // store the checkpoint
  virtual int store(const Checkpoint& _ckpt);

  // retrieve the checkpoint
  virtual int restore(Checkpoint& _ckpt);

 private:
  // get the name of the checkpoint directory
  std::string ckptDirectory(const Checkpoint& _ckpt);

  // get the name of the checkpoint file
  std::string ckptFile(const Checkpoint& _ckpt);

  // get the name of the temporary checkpoint file
  std::string tempCkptFile(const Checkpoint& _ckpt);

  // create the checkpoint directory
  int createCkptDirectory(const Checkpoint& _ckpt);

  // move the temporary checkpoint file
  int moveTmpCkptFile(const Checkpoint& _ckpt);

 private:
  // generate the log message prefix/suffix for printing
  std::string logMessageFragment(const Checkpoint& _ckpt);

 private:
  std::string   base_dir_;
};

}  // namespace state
}  // namespace heron

#endif  // lfs.h
