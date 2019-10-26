/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

////////////////////////////////////////////////////////////////////
//
// Utility functions for getting metadata about a process
//
/////////////////////////////////////////////////////////////////////

#if !defined(__PROCESS_UTILS_H)
#define __PROCESS_UTILS_H

#include <sys/types.h>
#include "basics/sptypes.h"

struct rusage;

class ProcessUtils {
 public:
  // get the PID of the process
  static pid_t getPid();

  // get the resource usage of the process
  static int getResourceUsage(struct rusage *usage);

  // get the total amount of memory used by the process
  static size_t getTotalMemoryUsed();

  // get working directory
  static sp_string getCurrentWorkingDirectory();
};

#endif
