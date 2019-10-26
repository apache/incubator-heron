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
#include <sstream>
#include <string>
#include <vector>
#include "core/zk/public/zkclient.h"
#include "core/network/public/event_loop_impl.h"

std::string topdir = "/";
std::string testtopdir = "/";
sp_uint32 nchildren = 10;
sp_uint32 children_notifications = 0;
std::shared_ptr<EventLoopImpl> ss;
ZKClient* zk_client = NULL;

void DeleteDone(sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "DeleteChildren failed with status " << _rc << std::endl;
    ::exit(1);
  }
  children_notifications++;
  if (children_notifications == nchildren + 1) {
    std::cout << "All tests passed\n";
    ss->loopExit();
  }
}

void GetChildrenDone(std::vector<std::string>* _result, sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "GetChildren failed with status " << _rc << std::endl;
    ::exit(1);
  }
  if (_result->size() != nchildren) {
    std::cerr << "Expected nchildren to be " << nchildren << " but got " << _result->size()
              << std::endl;
    ::exit(1);
  }
  // TODO:- do we need to do more checking with the results.
  delete _result;
  // Now delete everything
  children_notifications = 0;
  for (sp_uint32 i = 0; i < nchildren; ++i) {
    std::ostringstream ostr;
    ostr << testtopdir << "/" << i;
    zk_client->DeleteNode(ostr.str(), [](sp_uint32 rc) { DeleteDone) rc); });
  }
  zk_client->DeleteNode(testtopdir, CreateCallback(&DeleteDone));
}

void Get2Done(std::string* _result, sp_uint32 _i, sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "Get2 failed with status " << _rc << std::endl;
    ::exit(1);
  }
  std::ostringstream vstr;
  vstr << "value2-" << _i;
  if (vstr.str() != *_result) {
    std::cerr << "In Get2 expected result " << vstr.str() << " but got " << *_result << std::endl;
    ::exit(1);
  }
  delete _result;
  children_notifications++;
  if (children_notifications == nchildren) {
    std::cout << "All get2 done\n";
    children_notifications = 0;
    // All gets done. Now some get children
    std::vector<std::string>* result = new std::vector<std::string>();
    zk_client->GetChildren(testtopdir, result, [=](sp_uint32 rc) { GetChildrenDone(result, rc); });
  }
}

void SetDone(sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "Set failed with status " << _rc << std::endl;
    ::exit(1);
  }
  children_notifications++;
  if (children_notifications == nchildren) {
    std::cout << "All sets done\n";
    // now do some gets to verify
    children_notifications = 0;
    for (sp_uint32 i = 0; i < nchildren; ++i) {
      std::ostringstream ostr;
      ostr << testtopdir << "/" << i;
      std::string* result = new std::string();
      zk_client->Get(ostr.str(), result, [=](sp_uint32 rc) { Get2Done(result, i, rc); });
    }
  }
}

void GetDone(std::string* _result, sp_uint32 _i, sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "Get failed with status " << _rc << std::endl;
    ::exit(1);
  }
  std::ostringstream vstr;
  vstr << "value-" << _i;
  if (vstr.str() != *_result) {
    std::cerr << "In Get expected result " << vstr.str() << " but got " << *_result << std::endl;
    ::exit(1);
  }
  delete _result;
  children_notifications++;
  if (children_notifications == nchildren) {
    // All gets done. Now to some sets
    std::cout << "All gets done\n";
    children_notifications = 0;
    for (sp_uint32 i = 0; i < nchildren; ++i) {
      std::ostringstream ostr;
      ostr << testtopdir << "/" << i;
      std::ostringstream vstr;
      vstr << "value2-" << i;
      zk_client->Set(ostr.str(), vstr.str(), [](sp_uint32 rc) { SetDone(rc); });
    }
  }
}

void ChildrenCreateDone(sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "CreateChildren failed with status " << _rc << std::endl;
    ::exit(1);
  }
  children_notifications++;
  if (children_notifications == nchildren) {
    // All children created
    std::cout << "All children created\n";
    // Now start some get calls
    children_notifications = 0;
    for (sp_uint32 i = 0; i < nchildren; ++i) {
      std::ostringstream ostr;
      ostr << testtopdir << "/" << i;
      std::string* result = new std::string();
      zk_client->Get(ostr.str(), result, [=](sp_uint32 rc) { GetDone(result, i, rc); });
    }
  }
}

void TopDirCreateDone(sp_int32 _rc) {
  if (_rc != 0) {
    std::cerr << "TopDirCreate failed with status " << _rc << std::endl;
    ::exit(1);
  }
  for (sp_uint32 i = 0; i < nchildren; ++i) {
    std::ostringstream ostr;
    ostr << testtopdir << "/" << i;
    std::ostringstream vstr;
    vstr << "value-" << i;
    zk_client->CreateNode(ostr.str(), vstr.str(), false,
                          [](sp_uint32 rc) { ChildrenCreateDone(rc); });
  }
}

void WatchEventHandler(ZKClient::ZkWatchEvent event) {
  std::cout << "Watch event received: " << std::endl;
  std::cout << "Type: " << ZKClient::type2String(event.type) << std::endl;
  std::cout << "State: " << ZKClient::state2String(event.state) << std::endl;
  ss->loopExit();
}

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <hostportlist> <topleveldir>" << std::endl;
    exit(1);
  }
  topdir = argv[2];
  if (topdir[topdir.size() - 1] == '/') {
    // trim it
    topdir = std::string(topdir, 0, topdir.size() - 1);
  }
  testtopdir = topdir + "/simplezktest";

  ss = std::make_shared<EventLoopImpl>();
  zk_client = new ZKClient(argv[1], ss);
  zk_client->CreateNode(testtopdir, "Created as part of the unittests", false,
                        [](sp_uint32 rc) { TopDirCreateDone(rc); });
  ss->loop();
  delete zk_client;

  /*************** Test Session expiry with NO client global watch. *********/
  // ss = new EventLoopImpl();
  // zk_client = new ZKClient(argv[1], ss);
  // ss->loop();
  // Create a local partition for greater than timeout (30s), to verify that
  // session expired event is triggered, and without a client global watch,
  // the program should exit with a log fatal.
  // On Mac, Create partition
  // "sudo ipfw add 00993 deny tcp from any to any dst-port 2181"
  // Delete partition
  // "sudo ipfw delete 00993"

  /*************** Test Session expiry with client global watch. **********/
  // ss = new EventLoopImpl();
  // zk_client = new ZKClient(argv[1], ss, CreateCallback(&WatchEventHandler));
  // ss->loop();
  // Create a local partition for greater than timeout (30s), to verify that
  // session expired event is triggered, and client global watch callback should
  // be called.
  // delete zk_client;
  // delete ss;

  return 0;
}
