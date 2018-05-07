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
// topology_config_vars.h
//
// This mirrors the api/Config.java. Please update them together
// TODO(kramasamy): Can we do some autogeneration of both C++ and Java together
//
// Essentially this file defines as the config variables users
// can set as part of their topology config
///////////////////////////////////////////////////////////////
#ifndef TOPOLOGY_CONFIG_VARS_H_
#define TOPOLOGY_CONFIG_VARS_H_

namespace heron {
namespace config {

class TopologyConfigVars {
 public:
  static const sp_string TOPOLOGY_DEBUG;
  static const sp_string TOPOLOGY_STMGRS;
  static const sp_string TOPOLOGY_MESSAGE_TIMEOUT_SECS;
  static const sp_string TOPOLOGY_COMPONENT_PARALLELISM;
  static const sp_string TOPOLOGY_MAX_SPOUT_PENDING;
  static const sp_string TOPOLOGY_WORKER_CHILDOPTS;
  static const sp_string TOPOLOGY_SERIALIZER_CLASSNAME;
  static const sp_string TOPOLOGY_TICK_TUPLE_FREQ_SECS;
  static const sp_string TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS;
  // This is deprecated. Will be removed from future releases
  static const sp_string TOPOLOGY_ENABLE_ACKING;
  enum TopologyReliabilityMode {
    ATMOST_ONCE,
    ATLEAST_ONCE,
    EFFECTIVELY_ONCE
  };
  static const sp_string TOPOLOGY_RELIABILITY_MODE;
  static const sp_string TOPOLOGY_CONTAINER_CPU_REQUESTED;
  static const sp_string TOPOLOGY_CONTAINER_RAM_REQUESTED;
  static const sp_string TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL_SECONDS;
  static const sp_string TOPOLOGY_STATEFUL_START_CLEAN;
  static const sp_string TOPOLOGY_DROPTUPLES_UPON_BACKPRESSURE;
  static const sp_string TOPOLOGY_NAME;
  static const sp_string TOPOLOGY_TEAM_NAME;
  static const sp_string TOPOLOGY_TEAM_EMAIL;
  static const sp_string TOPOLOGY_CAP_TICKET;
  static const sp_string TOPOLOGY_PROJECT_NAME;
  static const sp_string TOPOLOGY_COMPONENT_OUTPUT_BPS;
};
}  // namespace config
}  // namespace heron

#endif
