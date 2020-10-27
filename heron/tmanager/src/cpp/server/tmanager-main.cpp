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

#include <iostream>
#include <string>
#include <vector>
#include "gflags/gflags.h"
#include "manager/tmanager.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

DEFINE_string(topology_name, "", "Name of the topology");
DEFINE_string(topology_id, "", "Id of the topology");
DEFINE_string(zkhostportlist, "", "Location of the zk");
DEFINE_string(zkroot, "", "Root of the zk");
DEFINE_string(myhost, "", "The hostname that I'm running");
DEFINE_int32(server_port, 0, "The port used for communication with stmgrs");
DEFINE_int32(controller_port, 0, "The port used to activate/deactivate");
DEFINE_int32(stats_port, 0, "The port of the getting stats");
DEFINE_string(config_file, "", "The heron internals config file");
DEFINE_string(override_config_file, "", "The override heron internals config file");
DEFINE_string(metrics_sinks_yaml, "", "The file that defines which sinks to send metrics");
DEFINE_int32(metricsmgr_port, 0, "The port of the local metrics manager");
DEFINE_int32(ckptmgr_port, 0, "The port of the local ckptmgr");


int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_zkhostportlist == "LOCALMODE") {
    FLAGS_zkhostportlist = "";
  }

  auto ss = std::make_shared<EventLoopImpl>();

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(ss,
    FLAGS_config_file, FLAGS_override_config_file);

  heron::common::Initialize(argv[0], FLAGS_topology_id.c_str());

  LOG(INFO) << "Starting tmanager for topology " << FLAGS_topology_name << " with topology id "
            << FLAGS_topology_id << " zkhostport " << FLAGS_zkhostportlist
            << " and zkroot " << FLAGS_zkroot;

  heron::tmanager::TManager tmanager(FLAGS_zkhostportlist, FLAGS_topology_name, FLAGS_topology_id,
                                  FLAGS_zkroot, FLAGS_controller_port, FLAGS_server_port,
                                  FLAGS_stats_port, FLAGS_metricsmgr_port,
                                  FLAGS_ckptmgr_port, FLAGS_metrics_sinks_yaml, FLAGS_myhost, ss);
  ss->loop();
  return 0;
}
