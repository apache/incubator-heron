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

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/common-config-vars.h"

namespace heron {
namespace config {

const sp_string CommonConfigVars::CLUSTER = "heron.config.cluster";
const sp_string CommonConfigVars::ROLE = "heron.config.role";
const sp_string CommonConfigVars::ENVIRON = "heron.config.environ";
const sp_string CommonConfigVars::TOPOLOGY_NAME = "heron.topology.name";

}  // namespace config
}  // namespace heron
