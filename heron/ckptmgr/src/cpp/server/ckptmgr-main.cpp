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

#include <stdlib.h>

#include <iostream>
#include <string>

#include "basics/basics.h"
#include "network/network.h"
#include "proto/messages.h"
#include "lfs/lfs.h"
#include "manager/ckptmgr.h"

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

  heron::common::Initialize(argv[0], ckptmgr_id.c_str());

  std::string home_dir(::getenv("HOME"));
  home_dir.append("/").append(".herondata");
  home_dir.append("/").append("topologies");
  home_dir.append("/").append("local");
  home_dir.append("/").append(::getenv("USER"));
  home_dir.append("/").append(topology_name);
  home_dir.append("/").append("state");

  heron::ckptmgr::Storage* storage = new heron::ckptmgr::LFS(home_dir);
  heron::ckptmgr::CkptMgr mgr(&ss, my_port, topology_name, topology_id, ckptmgr_id, storage);

  mgr.Init();
  ss.loop();
  return 0;
}
