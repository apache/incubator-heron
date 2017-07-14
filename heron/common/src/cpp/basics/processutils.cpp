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

#include "basics/processutils.h"
#include <sys/resource.h>
#include <unistd.h>
#include <gperftools/malloc_extension.h>

pid_t ProcessUtils::getPid() { return ::getpid(); }

int ProcessUtils::getResourceUsage(struct rusage *usage) { return ::getrusage(RUSAGE_SELF, usage); }

size_t ProcessUtils::getTotalMemoryUsed() {
  size_t total = 0;
  MallocExtension::instance()->GetNumericProperty("generic.heap_size", &total);
  return total;
}

sp_string ProcessUtils::getCurrentWorkingDirectory() {
  char buff[FILENAME_MAX];
  getcwd(buff, FILENAME_MAX);
  sp_string current_working_dir(buff);
  return current_working_dir;
}
