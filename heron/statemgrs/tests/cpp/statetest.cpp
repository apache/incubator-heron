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

#include "common/common.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "statemgr/heron-zkstatemgr.h"

using heron::common::HeronStateMgr;

void TManagerLocationWatchHandler() {
  std::cout << "TManagerLocationWatchHandler triggered " << std::endl;
}

int main(int argc, char* argv[]) {
  /*
   * This is not a test per se, just comes handy when trying out zookeeper changes.
   * It needs to be run with a local zookeeper.
   * To trigger a session expiry, a partition needs to be created
   * Create partition,
   * sudo ipfw add 00993 deny tcp from any to any dst-port 2181
   * Delete partition
   * sudo ipfw delete 00993
   * Console logs need to monitored to verify the behavior on seesion expiry.
   */
  EventLoopImpl ss;

  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <hostportlist> <topleveldir> " << std::endl;
    exit(1);
  }

  const std::string host_port = argv[1];
  const std::string top_level_dir = argv[2];
  const std::string topology_name = "test_topology";

  HeronStateMgr* state_mgr = HeronStateMgr::MakeStateMgr(host_port, top_level_dir, &ss);
  state_mgr->SetTManagerLocationWatch(topology_name, []() { TManagerLocationWatchHandler(); });
  state_mgr->SetPackingPlanWatch(topology_name, []() { PackingPlanWatchHandler(); });
  ss.loop();
  return 0;
}
