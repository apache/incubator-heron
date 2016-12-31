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
#include "manager/ckptmgr.h"
#include "basics/basics.h"
#include "network/network.h"
#include "proto/messages.h"

int main(int argc, char* argv[]) {
  if (argc != 5) {
    std::cout << "Usage: " << argv[0] << " "
              << "<topname> <topid> <ckptmgr_id> <myport>"
              << std::endl;
    ::exit(1);
  }

  std::string topology_name = argv[1];
  std::string topology_id = argv[2];
  std::string ckptmgr_id = argv[3];
  sp_int32 my_port = atoi(argv[4]);
  EventLoopImpl ss;

  heron::ckptmgr::CkptMgr mgr(&ss, my_port, topology_name, topology_id, ckptmgr_id);

  mgr.Init();
  ss.loop();
  return 0;
}
