/*
 * Copyright 2017 Twitter, Inc.
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

#include <stdlib.h>

#include <iostream>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

#include "gateway/gateway.h"
#include "slave/slave.h"

int main(int argc, char* argv[]) {
  if (argc != 13) {
    std::cout << "Usage: " << argv[0] << " "
              << "<topname> <topid> <instance_id> "
              << "<component_name> <task_id> <component_index> <stmgr_id>"
              << "<stmgr_port> <metricsmgr_port> <heron_internals_config_filename>"
              << "<override_config_filename> <topology_so>";
    ::exit(1);
  }

  std::string topologyName = argv[1];
  std::string topologyId = argv[2];
  std::string instanceId = argv[3];
  std::string componentName = argv[4];
  int taskId = atoi(argv[5]);
  int componentIndex = atoi(argv[6]);
  std::string stmgrId = argv[7];
  int stmgrPort = atoi(argv[8]);
  int metricsMgrPort = atoi(argv[9]);
  std::string heron_internals_config_filename = argv[10];
  std::string heron_override_config_filename = argv[11];
  std::string topologySo = argv[12];

  heron::common::Initialize(argv[0], instanceId.c_str());

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  EventLoopImpl eventLoop;
  heron::config::HeronInternalsConfigReader::Create(&eventLoop, heron_internals_config_filename,
                                                    heron_override_config_filename);

  auto gateway = new heron::instance::Gateway(topologyName, topologyId, instanceId,
                                              componentName, taskId, componentIndex,
                                              stmgrId, stmgrPort, metricsMgrPort, &eventLoop);
  auto slave = new heron::instance::Slave(taskId, topologySo);

  auto dataToSlave = new heron::instance::NotifyingCommunicator<google::protobuf::Message*>(
                               slave->eventLoop(),
                               std::bind(&heron::instance::Slave::HandleGatewayData,
                                         slave, std::placeholders::_1),
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleSlaveDataConsumed,
                                         gateway));

  auto dataFromSlave = new heron::instance::NotifyingCommunicator<google::protobuf::Message*>(
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleSlaveData,
                                         gateway, std::placeholders::_1),
                               slave->eventLoop(),
                               std::bind(&heron::instance::Slave::HandleGatewayDataConsumed,
                                         slave));

  auto metricsFromSlave = new heron::instance::NotifyingCommunicator<google::protobuf::Message*>(
                               gateway->eventLoop(),
                               std::bind(&heron::instance::Gateway::HandleSlaveMetrics,
                                         gateway, std::placeholders::_1),
                               slave->eventLoop(),
                               std::bind(&heron::instance::Slave::HandleGatewayMetricsConsumed,
                                         slave));

  gateway->setCommunicators(dataToSlave, dataFromSlave, metricsFromSlave);
  slave->setCommunicators(dataToSlave, dataFromSlave, metricsFromSlave);
  slave->Start();  // goes off to a thread
  gateway->Start();  // never returns
  return 0;
}
