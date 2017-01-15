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

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "state/heron-statemgr.h"
#include "dummytmaster.h"

int main(int argc, char* argv[]) {
  if (argc != 7) {
    std::cout
        << "Usage: " << argv[0]
        << " <host> <master-port> <topology-name> <zk_hostport> <zkroot> <sgmrid:host:port,...>"
        << std::endl;
    std::cout << "If zk_hostportlist is empty please say LOCALMODE\n";
    ::exit(1);
  }
  std::string myhost = argv[1];
  sp_int32 master_port = atoi(argv[2]);
  std::string topology_name = argv[3];
  std::string zkhostportlist = argv[4];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  std::string topdir = argv[5];
  std::vector<std::string> stmgrs = StrUtils::split(argv[6], ",");

  EventLoopImpl ss;
  NetworkOptions options;
  options.set_host(myhost);
  options.set_port(master_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);
  heron::tmaster::TMasterServer tmaster(&ss, options, topology_name, zkhostportlist, topdir,
                                        stmgrs);
  ss.loop();
  return 0;
}
