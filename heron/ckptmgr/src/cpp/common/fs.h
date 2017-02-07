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

#if !defined(FILE_SYSTEM_H)
#define FILE_SYSTEM_H

#include <unistd.h>
#include <string>

namespace heron {
namespace state {

class FS {
 public:
  // constructor
  FS() {}

  // destructor
  virtual ~FS() {}

  // open the file
  virtual int open(const char* path, int flags) = 0;

  // write data into the file
  virtual int write(int fd, const void* buf, size_t nbyte) = 0;

  // read data from the file
  virtual int read(int fd, void* buf, size_t nbyte) = 0;

  // close the file opened
  virtual int close(int fd) = 0;
};

}  // namespace state
}  // namespace heron

#endif  // fs.h
