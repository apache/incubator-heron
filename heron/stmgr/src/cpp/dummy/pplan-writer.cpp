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

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/helper.h"
#include "state/heron-statemgr.h"

heron::common::HeronStateMgr* state_mgr = NULL;

void GeneratePhysicalPlan(heron::proto::system::PhysicalPlan& _pplan,
                          heron::proto::api::Topology* _topology) {
  // Copy the topology verbatim
  _pplan.mutable_topology()->CopyFrom(*_topology);

  // Just one stmgr
  heron::proto::system::StMgr* stmgr = _pplan.add_stmgrs();
  stmgr->set_id("stmgr");
  stmgr->set_host_name("127.0.0.1");
  stmgr->set_data_port(8888);
  stmgr->set_local_endpoint("/notused");

  // Just two instances
  heron::proto::system::Instance* wrkr1 = _pplan.add_instances();
  wrkr1->set_instance_id("worker1");
  wrkr1->set_stmgr_id("stmgr");
  heron::proto::system::InstanceInfo* instance1 = wrkr1->mutable_info();
  instance1->set_task_id(0);
  instance1->set_component_index(0);
  instance1->set_component_name("word");

  heron::proto::system::Instance* wrkr2 = _pplan.add_instances();
  wrkr2->set_instance_id("worker2");
  wrkr2->set_stmgr_id("stmgr");
  heron::proto::system::InstanceInfo* instance2 = wrkr2->mutable_info();
  instance2->set_task_id(0);
  instance2->set_component_index(0);
  instance2->set_component_name("exclaim1");
}

void OnCreatePlan(EventLoop* eventLoop, heron::proto::system::StatusCode _code) {
  if (_code != heron::proto::system::OK) {
    LOG(ERROR) << "Error creating pplan " << _code << std::endl;
    ::exit(1);
  }
  _ss->loopExit();
}

void OnGetTopology(heron::proto::api::Topology* _topology, EventLoop* eventLoop,
                   heron::proto::system::StatusCode _code) {
  if (_code != heron::proto::system::OK) {
    LOG(ERROR) << "Error getting topology " << _code << std::endl;
    ::exit(1);
  }
  heron::proto::system::PhysicalPlan pplan;
  GeneratePhysicalPlan(pplan, _topology);
  state_mgr->CreatePhysicalPlan(pplan, CreateCallback(&OnCreatePlan, eventLoop));
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    std::cout << "Usage: " << argv[0] << " <zkhostportlist> <topleveldir> <topologyname>"
              << std::endl;
    std::cout << "If you want to use local filesystem and not zk, please substibute LOCALMODE for "
                 "zkhostportlist\n";
    ::exit(1);
  }

  std::string zkhostport = argv[1];
  if (zkhostport == "LOCALMODE") {
    zkhostport = "";
  }
  std::string topdir = argv[2];
  std::string topname = argv[3];

  EventLoop* eventLoop = new EventLoopImpl();
  state_mgr = heron::common::HeronStateMgr::MakeStateMgr(zkhostport, topdir, ss);
  heron::proto::api::Topology* topology = new heron::proto::api::Topology();
  state_mgr->GetTopology(topname, topology, CreateCallback(&OnGetTopology, topology, ss));
  ss->loop();
  LOG(INFO) << "Done writing physical plan" << std::endl;
  delete ss;
  delete state_mgr;
  delete topology;
  return 0;
}
