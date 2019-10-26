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
#include "manager/stmgr.h"
#include "statemgr/heron-statemgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

DEFINE_string(topology_name, "", "Name of the topology");
DEFINE_string(topology_id, "", "Id of the topology");
DEFINE_string(topologydefn_file, "", "Name of the topology defn file");
DEFINE_string(zkhostportlist, "", "Location of the zk");
DEFINE_string(zkroot, "", "Root of the zk");
DEFINE_string(stmgr_id, "", "My Id");
DEFINE_string(instance_ids, "", "Comma seperated list of instance ids in my container");
DEFINE_string(myhost, "", "The hostname that I'm running");
DEFINE_int32(data_port, 0, "The port used for inter-container traffic");
DEFINE_int32(local_data_port, 0, "The port used for intra-container traffic");
DEFINE_int32(metricsmgr_port, 0, "The port of the local metricsmgr");
DEFINE_int32(shell_port, 0, "The port of the local heron shell");
DEFINE_string(config_file, "", "The heron internals config file");
DEFINE_string(override_config_file, "", "The override heron internals config file");
DEFINE_string(ckptmgr_id, "", "The id of the local ckptmgr");
DEFINE_int32(ckptmgr_port, 0, "The port of the local ckptmgr");
DEFINE_string(metricscachemgr_mode, "disabled", "MetricsCacheMgr mode, default `disabled`");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_zkhostportlist == "LOCALMODE") {
    FLAGS_zkhostportlist = "";
  }
  std::vector<std::string> instances = StrUtils::split(FLAGS_instance_ids, ",");

  auto ss = std::make_shared<EventLoopImpl>();

  // Read heron internals config from local file
  // Create the heron-internals-config-reader to read the heron internals config
  heron::config::HeronInternalsConfigReader::Create(ss,
    FLAGS_config_file, FLAGS_override_config_file);

  heron::common::Initialize(argv[0], FLAGS_stmgr_id.c_str());

  // Lets first read the top defn file
  auto topology = std::make_shared<heron::proto::api::Topology>();
  sp_string contents = FileUtils::readAll(FLAGS_topologydefn_file);
  topology->ParseFromString(contents);
  if (!topology->IsInitialized()) {
    LOG(FATAL) << "Corrupt topology defn file " << FLAGS_topologydefn_file;
  }

  sp_int64 high_watermark = heron::config::HeronInternalsConfigReader::Instance()
                              ->GetHeronStreammgrNetworkBackpressureHighwatermarkMb() *
                                1_MB;
  sp_int64 low_watermark = heron::config::HeronInternalsConfigReader::Instance()
                              ->GetHeronStreammgrNetworkBackpressureLowwatermarkMb() *
                                1_MB;
  heron::stmgr::StMgr mgr(ss, FLAGS_myhost, FLAGS_data_port, FLAGS_local_data_port,
                          FLAGS_topology_name, FLAGS_topology_id, topology, FLAGS_stmgr_id,
                          instances, FLAGS_zkhostportlist, FLAGS_zkroot, FLAGS_metricsmgr_port,
                          FLAGS_shell_port, FLAGS_ckptmgr_port, FLAGS_ckptmgr_id,
                          high_watermark, low_watermark, FLAGS_metricscachemgr_mode);
  mgr.Init();
  ss->loop();
  return 0;
}
