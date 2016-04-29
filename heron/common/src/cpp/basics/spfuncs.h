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
// Definition various global initialization functions
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_FUNCS_H_)
#define HERON_FUNCS_H_

namespace heron {
namespace common {

// Heron initialize with instance
void Initialize(const char* argv0, const char* instance);

// Heron initialize - no instance
void Initialize(const char* argv0);

// Heron Shutdown
void Shutdown();

// Heron function for log pruning
void PruneLogs();

// Heron function for log flushing
void FlushLogs();
}  // namespace common
}  // namespace heron

#endif  // HERON_FUNCS_H_
