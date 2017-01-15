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

#include "common/common.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "state/heron-statemgr.h"
#include "dummy/stmgr.h"

void SplitStringUsing(std::string _input, const std::string& _delim,
                      std::vector<std::string>& _return) {
  size_t pos = 0;
  std::string token;
  while ((pos = _input.find(_delim)) != std::string::npos) {
    token = _input.substr(0, pos);
    _return.push_back(token);
    _input.erase(0, pos + _delim.length());
  }
  if (_input.size() > 0) {
    _return.push_back(_input);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 9) {
    std::cout
        << "Usage: " << argv[0]
        << " <topname> <topid> <zknode> <zkroot> <stmgrid> <spoutworkerids> <boltworkerids> <port>"
        << std::endl;
    std::cout << "If zknode is empty please say LOCALMODE\n";
    ::exit(1);
  }
  std::string topology_name = argv[1];
  std::string topology_id = argv[2];
  std::string zkhostportlist = argv[3];
  if (zkhostportlist == "LOCALMODE") {
    zkhostportlist = "";
  }
  std::string topdir = argv[4];
  std::string myid = argv[5];
  std::string spout_workerids = argv[6];
  std::vector<std::string> spout_workers;
  SplitStringUsing(spout_workerids, ",", spout_workers);
  std::string bolt_workerids = argv[7];
  std::vector<std::string> bolt_workers;
  SplitStringUsing(bolt_workerids, ",", bolt_workers);
  sp_int32 myport = atoi(argv[8]);

  google::InitGoogleLogging(argv[0]);

  EventLoopImpl ss;
  NetworkOptions sops;
  sops.set_host("localhost");  // this is mostly ignored
  sops.set_port(myport);
  sops.set_socket_family(PF_INET);
  sops.set_max_packet_size(std::numeric_limits<sp_uint32>::max() - 1);

  heron::stmgr::StMgr mgr(&ss, sops, topology_name, myid, spout_workers, bolt_workers,
                          zkhostportlist, topdir);
  mgr->Init();
  ss.loop();
  return 0;
}
