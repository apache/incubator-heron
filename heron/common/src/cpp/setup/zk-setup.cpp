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

//////////////////////////////////////////////////////////////////////////////
//
// zk-setup.h
//
// This file sets up zookeeper for Heron. It essentially
// creates the various directories needed for heron usage.
//////////////////////////////////////////////////////////////////////////////

#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "zookeeper/zkclient.h"

ZKClient* zkclient = NULL;
sp_string clustername;
sp_string zkroot;

void usage() {
  std::cout << "zk-setup <clustername> <zkserverhostportlist> <zkrootdir>" << std::endl;
}

void AllDone(sp_int32 _status) {
  if (_status == ZNODEEXISTS || _status == ZOK) {
    LOG(INFO) << "Successfully set zk" << std::endl;
    delete zkclient;
    ::exit(0);
  } else {
    LOG(ERROR) << "Error creating node in zk " << _status << std::endl;
    ::exit(1);
  }
}

void TManagersDone(sp_int32 _status) {
  if (_status == ZNODEEXISTS || _status == ZOK) {
    zkclient->CreateNode(zkroot + "/executionstate", "Heron Cluster " + clustername, false,
                         [](sp_int32 status) { AllDone(status); });
  } else {
    LOG(ERROR) << "Error creating node in zk " << _status << std::endl;
    ::exit(1);
  }
}

void PplansDone(sp_int32 _status) {
  if (_status == ZNODEEXISTS || _status == ZOK) {
    zkclient->CreateNode(zkroot + "/tmanagers", "Heron Cluster " + clustername, false,
                         [](sp_int32 status) { TManagersDone(status); });
  } else {
    LOG(ERROR) << "Error creating node in zk " << _status << std::endl;
    ::exit(1);
  }
}

void TopologyDone(sp_int32 _status) {
  if (_status == ZNODEEXISTS || _status == ZOK) {
    zkclient->CreateNode(zkroot + "/pplans", "Heron Cluster " + clustername, false,
                         [](sp_int32 status) { PplansDone(status); });
  } else {
    LOG(ERROR) << "Error creating node in zk " << _status << std::endl;
    ::exit(1);
  }
}

void ZkRootDone(sp_int32 _status) {
  if (_status == ZNODEEXISTS || _status == ZOK) {
    zkclient->CreateNode(zkroot + "/topologies", "Heron Cluster " + clustername, false,
                         [](sp_int32 status) { TopologyDone(status); });
  } else {
    LOG(ERROR) << "Error creating node in zk " << _status << std::endl;
    ::exit(1);
  }
}

int main(int argc, char* argv[]) {
  if (argc != 4) {
    usage();
    exit(1);
  }

  clustername = argv[1];
  sp_string zkhostport = argv[2];
  zkroot = argv[3];
  // Make sure that zkroot starts with '/'
  if (zkroot.substr(0, 1) != "/") {
    LOG(ERROR) << "zkroot should start with /" << std::endl;
    ::exit(1);
  }
  // remove trailing '/'
  if (zkroot[zkroot.size() - 1] == '/') {
    zkroot = std::string(zkroot, 0, zkroot.size() - 1);
  }

  auto ss = std::make_shared<EventLoopImpl>();
  zkclient = new ZKClient(zkhostport, ss);

  zkclient->CreateNode(zkroot, "Heron Cluster " + clustername, false,
                       [](sp_int32 status) { ZkRootDone(status); });
  ss->loop();
}
