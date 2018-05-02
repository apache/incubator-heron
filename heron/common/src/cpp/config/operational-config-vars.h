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

////////////////////////////////////////////////////////////////
//
// operational-config-vars.h
//
// This is the operational configuration parameter names
//
// This file should be used for things like
// CAP tickets to machine mappings,
// topology_release_overrides, etc
///////////////////////////////////////////////////////////////
#ifndef OPERATIONAL_CONFIG_VARS_H_
#define OPERATIONAL_CONFIG_VARS_H_

namespace heron {
namespace config {

class OperationalConfigVars {
 public:
  static const sp_string TOPOLOGY_RELEASE_OVERRIDES;
};
}  // namespace config
}  // namespace heron

#endif
