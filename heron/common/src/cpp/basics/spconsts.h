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

////////////////////////////////////////////////////////////////////////////////
//
// This file contains the definitions of various constants used in the system
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_CONSTS_H_)
#define HERON_CONSTS_H_

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

#endif
