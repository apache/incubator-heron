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

#include <stdlib.h>

#include <iostream>
#include <string>
#include <vector>
#include "gflags/gflags.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

#include "gateway/gateway.h"
#include "executor/executor.h"

DEFINE_string(topology_name, "", "Name of the topology");
DEFINE_string(topology_id, "", "Id of the topology");
DEFINE_string(instance_id, "", "My Instance Id");
DEFINE_string(component_name, "", "My Component Name");
DEFINE_int32(task_id, 0, "My Task Id");
DEFINE_int32(component_index, 0, "The index of my component");
DEFINE_string(stmgr_id, "", "The Id of my stmgr");
DEFINE_int32(stmgr_port, 0, "The port used to communicate with my stmgr");
DEFINE_int32(metricsmgr_port, 0, "The port of the local metricsmgr");
DEFINE_string(config_file, "", "The heron internals config file");
DEFINE_string(override_config_file, "", "The override heron internals config file");
DEFINE_string(topology_binary, "", "The topology .so/dylib file");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  heron::common::Initialize(argv[0], FLAGS_instance_id.c_str());

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  auto eventLoop = std::make_shared<EventLoopImpl>();
  heron::config::HeronInternalsConfigReader::Create(eventLoop, FLAGS_config_file,
                                                    FLAGS_override_config_file);

  auto gateway = new heron::instance::Gateway(FLAGS_topology_name, FLAGS_topology_id,
                                              FLAGS_instance_id, FLAGS_component_name,
                                              FLAGS_task_id, FLAGS_component_index,
                                              FLAGS_stmgr_id, FLAGS_stmgr_port,
                                              FLAGS_metricsmgr_port, eventLoop);
  auto executor = new heron::instance::Executor(FLAGS_task_id, FLAGS_topology_binary);

  auto dataToExecutor =
          new heron::instance::NotifyingCommunicator<pool_unique_ptr<google::protobuf::Message>>(
                               executor->eventLoop(),
                               std::bind(&heron::instance::Executor::HandleGatewayData,
                                         executor, std::placeholders::_1),
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleExecutorDataConsumed,
                                         gateway));

  auto dataFromExecutor = new heron::instance::NotifyingCommunicator<google::protobuf::Message*>(
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleExecutorData,
                                         gateway, std::placeholders::_1),
                               executor->eventLoop(),
                               std::bind(&heron::instance::Executor::HandleGatewayDataConsumed,
                                         executor));

  auto metricsFromExecutor = new heron::instance::NotifyingCommunicator<google::protobuf::Message*>(
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleExecutorMetrics,
                                         gateway, std::placeholders::_1),
                               executor->eventLoop(),
                               std::bind(&heron::instance::Executor::HandleGatewayMetricsConsumed,
                                         executor));

  gateway->setCommunicators(dataToExecutor, dataFromExecutor, metricsFromExecutor);
  executor->setCommunicators(dataToExecutor, dataFromExecutor, metricsFromExecutor);
  executor->Start();  // goes off to a thread
  gateway->Start();  // never returns
  return 0;
}
