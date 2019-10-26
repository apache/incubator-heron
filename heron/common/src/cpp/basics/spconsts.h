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

////////////////////////////////////////////////////////////////////////////////
//
// This file contains the definitions of various constants used in the system
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_CONSTS_H_)
#define HERON_CONSTS_H_

#include "basics/sptypes.h"

// constants related to logs
constexpr auto constMaxNumLogFiles = 5;
constexpr auto constLogsDirectory = "log-files";
constexpr auto constTestLogsDirectory = ".";

// constants for files and directories
constexpr auto constCurrentDirectory = ".";
constexpr auto constParentDirectory = "..";
constexpr auto constPathSeparator = "/";

// constants for socket utilities
constexpr auto constTcpKeepAliveSecs = 300;          // 5 min
constexpr auto constTcpKeepAliveProbeInterval = 10;  // 10 seconds
constexpr auto constTcpKeepAliveProbes = 5;

constexpr sp_int64 operator"" _KB(unsigned long long kilobytes) { // NOLINT
    return (sp_int64)kilobytes * 1024;
}

constexpr sp_int64 operator"" _MB(unsigned long long megabytes) { // NOLINT
    return (sp_int64)megabytes * 1024 * 1024;
}

constexpr sp_int64 operator"" _s(unsigned long long sec) { // NOLINT
    return (sp_int64)sec * 1000000;
}

constexpr sp_int64 operator"" _ms(unsigned long long ms) { // NOLINT
    return (sp_int64)ms * 1000;
}

#endif
