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

#if !defined(STORAGE_H)
#define STORAGE_H

#include <string>

namespace heron {
namespace state {

class Storage {
 public:
  Storage() {}

  virtual ~Storage() {}

  // store the checkpoint
  virtual int store(const Checkpoint& _ckpt) = 0;

  // retrieve the checkpoint
  virtual int restore(Checkpoint& _ckpt) = 0;
};

}  // namespace state
}  // namespace heron

#endif  // ckpt.h
