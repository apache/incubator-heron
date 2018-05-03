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
// cluster-config-vars.h
//
// This is the cluster configuration parameter names
// This is the replica of worker/SystemConfig.java
// TODO(karamasamy): Can we autogenerate both C++/Java
//
// Essentially this file defines as the config variables cluster
// admin can set as part of their heron installation
///////////////////////////////////////////////////////////////
#ifndef CLUSTER_CONFIG_VARS_H_
#define CLUSTER_CONFIG_VARS_H_

namespace heron {
namespace config {

class ClusterConfigVars {
 public:
  static const sp_string CLUSTER_METRICS_INTERVAL;
};
}  // namespace config
}  // namespace heron

#endif
