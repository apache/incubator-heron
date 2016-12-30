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

#include "lfs/lfs.h"
#include <fcntl.h>
#include <string>

namespace heron {
namespace state {

int
LFS::open(const char* path, int flags) {
  return ::open(path, flags);
}

int
LFS::write(int fd, const void* buf, size_t nbytes) {
  return ::write(fd, buf, nbytes);
}

int
LFS::read(int fd, void* buf, size_t nbytes) {
  return ::read(fd, buf, nbytes);
}

int
LFS::close(int fd) {
  return ::close(fd);
}

}  // namespace state
}  // namespace heron
