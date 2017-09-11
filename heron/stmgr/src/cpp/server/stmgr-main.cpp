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
#include "manager/stmgr.h"
#include "statemgr/heron-statemgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

int main(int argc, char* argv[]) {
  if (argc != 15) {
    std::cout << "Usage: " << argv[0] << " "
              << "<topname> <topid> <topdefnfile> "
              << "<zknode> <zkroot> <stmgrid> "
              << "<instanceids> <myhost> <myport> <metricsmgrport> "
              << "<shellport> <heron_internals_config_filename> "
              << "<ckptmgr_port> <ckptmgr_id>"
              << std::endl;
    std::cout << "If zknode is empty please say LOCALMODE\n";
    ::exit(1);
  }

  std::string topology_name = argv[1];
  std::string topology_id = argv[2];
  std::string topdefn_file = argv[3];
  std::string zkhostportlist = argv[4];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  std::string topdir = argv[5];
  std::string myid = argv[6];
  std::string instanceids = argv[7];
  std::vector<std::string> instances = StrUtils::split(instanceids, ",");
  std::string myhost = argv[8];
  sp_int32 myport = atoi(argv[9]);
  sp_int32 metricsmgr_port = atoi(argv[10]);
  sp_int32 shell_port = atoi(argv[11]);
  sp_string heron_internals_config_filename = argv[12];
  sp_int32 ckptmgr_port = atoi(argv[13]);
  sp_string ckptmgr_id = argv[14];

  EventLoopImpl ss;

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(&ss, heron_internals_config_filename);

  heron::common::Initialize(argv[0], myid.c_str());

  // Lets first read the top defn file
  heron::proto::api::Topology* topology = new heron::proto::api::Topology();
  sp_string contents = FileUtils::readAll(topdefn_file);
  topology->ParseFromString(contents);
  if (!topology->IsInitialized()) {
    LOG(FATAL) << "Corrupt topology defn file" << std::endl;
  }

  sp_int64 high_watermark = heron::config::HeronInternalsConfigReader::Instance()
                              ->GetHeronStreammgrNetworkBackpressureHighwatermarkMb() *
                                1_MB;
  sp_int64 low_watermark = heron::config::HeronInternalsConfigReader::Instance()
                              ->GetHeronStreammgrNetworkBackpressureLowwatermarkMb() *
                                1_MB;
  heron::stmgr::StMgr mgr(&ss, myhost, myport, topology_name, topology_id, topology, myid,
                          instances, zkhostportlist, topdir, metricsmgr_port, shell_port,
                          ckptmgr_port, ckptmgr_id, high_watermark, low_watermark);
  mgr.Init();
  ss.loop();
  return 0;
}
