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
#include "config/override-config-reader.h"

void string_replace(sp_string & strBig, const sp_string & strsrc, const sp_string & strdst) {
  sp_string::size_type pos = 0;
  sp_string::size_type srclen = strsrc.size();
  sp_string::size_type dstlen = strdst.size();
  while ( (pos = strBig.find(strsrc, pos)) != sp_string::npos ) {
    strBig.replace(pos, srclen, strdst);
    pos += dstlen;
  }
}

int main(int argc, char* argv[]) {
  if (argc != 13) {
    std::cout << "Usage: " << argv[0] << " "
              << "<master-host> <master-port> <controller-port> <stats-port> "
              << "<topology_name> <topology_id> <zk_hostportlist> "
              << "<topdir> <sgmr1,...> <heron_internals_config_filename> "
              << "<metrics_sinks_filename> <metrics-manager-port>" << std::endl;
    std::cout << "If zk_hostportlist is empty please say LOCALMODE\n";
    ::exit(1);
  }

  sp_string myhost = argv[1];
  sp_int32 master_port = atoi(argv[2]);
  sp_int32 controller_port = atoi(argv[3]);
  sp_int32 stats_port = atoi(argv[4]);
  sp_string topology_name = argv[5];
  sp_string topology_id = argv[6];
  sp_string zkhostportlist = argv[7];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  sp_string topdir = argv[8];
  std::vector<std::string> stmgrs = StrUtils::split(argv[9], ",");
  sp_string heron_internals_config_filename = argv[10];
  sp_string metrics_sinks_yaml = argv[11];
  sp_int32 metrics_manager_port = atoi(argv[12]);

  EventLoopImpl ss;

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(&ss, heron_internals_config_filename);

  heron::common::Initialize(argv[0], topology_id.c_str());

  LOG(INFO) << "Starting tmaster for topology " << topology_name << " with topology id "
            << topology_id << " zkhostport " << zkhostportlist << " zkroot " << topdir
            << " and nstmgrs " << stmgrs.size() << std::endl;

  sp_string override_config_filename =  sp_string(heron_internals_config_filename);
  string_replace(override_config_filename, "heron_internals.yaml", "override.yaml");
  heron::config::OverrideConfigReader::Create(override_config_filename);
  heron::config::OverrideConfigReader* override_config_reader =
      heron::config::OverrideConfigReader::Instance();
  // feature: auto restart backpressure container
  // backpressue_window > 0: the time window size in minutes
  // backpressue_window <= 0: disable the feature
  sp_int32 backpressue_window = override_config_reader->GetHeronAutoHealWindow();
  sp_int32 backpressue_interval = override_config_reader->GetHeronAutoHealInterval();

  heron::tmaster::TMaster tmaster(zkhostportlist, topology_name, topology_id, topdir, stmgrs,
                                  controller_port, master_port, stats_port, metrics_manager_port,
                                  metrics_sinks_yaml, myhost, &ss, backpressue_window,
                                  backpressue_interval);
  ss.loop();
  return 0;
}
