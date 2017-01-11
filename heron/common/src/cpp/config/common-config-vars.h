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

////////////////////////////////////////////////////////////////
//
// common-config-vars.h
//
// This is the common configuration parameter names
//
///////////////////////////////////////////////////////////////
#ifndef COMMON_CONFIG_VARS_H_
#define COMMON_CONFIG_VARS_H_

namespace heron {
namespace config {

class CommonConfigVars {
 public:
  static const sp_string CLUSTER;
  static const sp_string ROLE;
  static const sp_string ENVIRON;
  static const sp_string TOPOLOGY_NAME;
};
}  // namespace config
}  // namespace heron

#endif
