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

#include <iostream>
#include <string>
#include <vector>
#include "manager/tmaster.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

int main(int argc, char* argv[]) {
  if (argc != 13) {
    std::cout << "Usage: " << argv[0] << " "
              << "<master-port> <controller-port> <stats-port> "
              << "<topology_name> <topology_id> <zk_hostportlist> "
              << "<topdir> <sgmr1,...> <heron_internals_config_filename> "
              << "<metrics_sinks_filename> <metrics-manager-port> "
              << "<auto_restart_backpressure_sandbox_time_window>" << std::endl;
    std::cout << "If zk_hostportlist is empty please say LOCALMODE\n";
    ::exit(1);
  }

  sp_string myhost = IpUtils::getHostName();
  sp_int32 master_port = atoi(argv[1]);
  sp_int32 controller_port = atoi(argv[2]);
  sp_int32 stats_port = atoi(argv[3]);
  sp_string topology_name = argv[4];
  sp_string topology_id = argv[5];
  sp_string zkhostportlist = argv[6];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  sp_string topdir = argv[7];
  std::vector<std::string> stmgrs = StrUtils::split(argv[8], ",");
  sp_string heron_internals_config_filename = argv[9];
  sp_string metrics_sinks_yaml = argv[10];
  sp_int32 metrics_manager_port = atoi(argv[11]);
  // STREAMCOMP-1877 auto restart backpressure sandbox
  // backpressue_window > 0: the time window size in minutes
  // backpressue_window <= 0: disable the feature
  sp_int32 backpressue_window = atoi(argv[12]);

  EventLoopImpl ss;

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(&ss, heron_internals_config_filename);

  heron::common::Initialize(argv[0], topology_id.c_str());

  LOG(INFO) << "Starting tmaster for topology " << topology_name << " with topology id "
            << topology_id << " zkhostport " << zkhostportlist << " zkroot " << topdir
            << " and nstmgrs " << stmgrs.size() << std::endl;

  heron::tmaster::TMaster tmaster(zkhostportlist, topology_name, topology_id, topdir, stmgrs,
                                  controller_port, master_port, stats_port, metrics_manager_port,
                                  metrics_sinks_yaml, myhost, &ss, backpressue_window);
  ss.loop();
  return 0;
}
