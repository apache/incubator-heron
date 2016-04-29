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

///////////////////////////////////////////////////////////////////////////////
//
// This file defines the implementation of a simple spin lock
//
////////////////////////////////////////////////////////////////////////////////

#if !defined(__ERROR_SPIN_H)
#define __ERROR_SPIN_H

namespace heron {
namespace error {

class __Spinlock {
 public:
  __Spinlock() : lock_(0) {}
  ~__Spinlock() {}

  void acquire() {
    while (__sync_lock_test_and_set(&lock_, 1)) {}
  }

  void release() { __sync_lock_release(&lock_); }

 private:
  sp_int32 lock_;
};
}  // namespace error
}  // namespace heron

#endif  // end of header file
