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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "zookeeper/mock_zkclient.h"
#include "zookeeper/zkclient_factory.h"
#include "statemgr/heron-zkstatemgr.h"

using ::testing::_;
using ::testing::AtLeast;
using ::testing::Mock;
using ::testing::InvokeWithoutArgs;

namespace heron {
namespace common {

using std::shared_ptr;

class MockZKClientFactory : public ZKClientFactory {
 public:
  MOCK_METHOD3(create, ZKClient*(const std::string& hostportlist, shared_ptr<EventLoop> eventLoop,
                                 VCallback<ZKClient::ZkWatchEvent> global_watcher_cb));
  MOCK_METHOD0(Die, void());
  virtual ~MockZKClientFactory() { Die(); }
};

// To inherit protected constructor through which a mock zkclient factory
// can be passed
class HeronZKStateMgrWithMock : public heron::common::HeronZKStateMgr {
 public:
  HeronZKStateMgrWithMock(const std::string& zkhostport, const std::string& topleveldir,
                          std::shared_ptr<EventLoop> eventLoop, ZKClientFactory* zkclient_factory)
      : HeronZKStateMgr(zkhostport, topleveldir, eventLoop, zkclient_factory) {}

  virtual ~HeronZKStateMgrWithMock() {}
};

// A friend class of HeronZKStateMgr
class HeronZKStateMgrTest : public ::testing::Test {
 public:
  // Initialize common test variables which gets run for each test
  void SetUp() {
    mock_zkclient_factory = new MockZKClientFactory();
    hostportlist = "dummy_hostport";
    topleveldir = "dummy_topleveldir";

    // mock zkclient factory, on create call returns a mock zkclient
    ON_CALL(*mock_zkclient_factory, create(_, _, _))
        .WillByDefault(InvokeWithoutArgs(this, &HeronZKStateMgrTest::makeNewMockClient));
  }

 protected:
  MockZKClient* makeNewMockClient() {
    mock_zkclient = new MockZKClient();
    return mock_zkclient;
  }
  // a proxy for the call since the tests cannot call directly
  // (friendship inheritance is not supported)
  static void CallTManagerLocationWatch(HeronZKStateMgr* heron_zkstatemgr) {
    heron_zkstatemgr->TManagerLocationWatch();
  }
  static void CallPackingPlanWatch(HeronZKStateMgr* heron_zkstatemgr) {
    heron_zkstatemgr->PackingPlanWatch();
  }

  // a proxy for the call since the tests cannot call directly
  // (friendship inheritance is not supported)
  static void CallGlobalWatchEventHandler(HeronZKStateMgr* heron_zkstatemgr,
                                          ZKClient::ZkWatchEvent event) {
    heron_zkstatemgr->GlobalWatchEventHandler(event);
  }

  static void TmanagerLocationWatchHandler() { tmanager_watch_handler_count++; }
  static void PackingPlanWatchHandler() { packing_plan_watch_handler_count++; }

  MockZKClient* mock_zkclient;
  MockZKClientFactory* mock_zkclient_factory;
  std::shared_ptr<EventLoop> ss;
  std::string hostportlist;
  std::string topleveldir;
  // used to verify the number of calls to TmanagerLocationWatchHandler
  static int tmanager_watch_handler_count;
  static int packing_plan_watch_handler_count;
};

// static member needs to be defined outside class... sigh :(
int HeronZKStateMgrTest::tmanager_watch_handler_count = 0;
int HeronZKStateMgrTest::packing_plan_watch_handler_count = 0;

// Ensure that ZKClient is created and deleted appropriately.
TEST_F(HeronZKStateMgrTest, testCreateDelete) {
  // Calling the factory create method ensures that ZkClient is created once
  EXPECT_CALL(*mock_zkclient_factory, create(hostportlist, ss, _)).Times(1);

  HeronZKStateMgr* heron_zkstatemgr =
      new HeronZKStateMgrWithMock(hostportlist, topleveldir, ss, mock_zkclient_factory);

  // Ensure zkclient is deleted
  EXPECT_CALL(*mock_zkclient, Die()).Times(1);
  // Ensure zkclient_factory is deleted
  EXPECT_CALL(*mock_zkclient_factory, Die()).Times(1);

  delete heron_zkstatemgr;
}

TEST_F(HeronZKStateMgrTest, testSetTManagerLocationWatch) {
  const std::string topology_name = "dummy_topology";
  const std::string expected_path = topleveldir + "/tmanagers/" + topology_name;

  // Calling the factory create method ensures that ZkClient is created once
  EXPECT_CALL(*mock_zkclient_factory, create(hostportlist, ss, _)).Times(1);

  HeronZKStateMgr* heron_zkstatemgr =
      new HeronZKStateMgrWithMock(hostportlist, topleveldir, ss, mock_zkclient_factory);

  // Ensure that it sets a watch to the tmanager location
  EXPECT_CALL(*mock_zkclient, Exists(expected_path, _, _)).Times(1);

  heron_zkstatemgr->SetTManagerLocationWatch(topology_name, []() { TmanagerLocationWatchHandler(); });

  EXPECT_CALL(*mock_zkclient, Die()).Times(1);
  EXPECT_CALL(*mock_zkclient_factory, Die()).Times(1);

  delete heron_zkstatemgr;
}

TEST_F(HeronZKStateMgrTest, testTManagerLocationWatch) {
  const std::string topology_name = "dummy_topology";
  const std::string expected_path = topleveldir + "/tmanagers/" + topology_name;

  HeronZKStateMgr* heron_zkstatemgr =
      new HeronZKStateMgrWithMock(hostportlist, topleveldir, ss, mock_zkclient_factory);

  heron_zkstatemgr->SetTManagerLocationWatch(topology_name, []() { TmanagerLocationWatchHandler(); });

  // ensure TmanagerLocationWatch resets the watch
  EXPECT_CALL(*mock_zkclient, Exists(expected_path, _, _)).Times(1);

  tmanager_watch_handler_count = 0;
  CallTManagerLocationWatch(heron_zkstatemgr);
  // ensure watch handler is called.
  ASSERT_EQ(tmanager_watch_handler_count, 1);

  EXPECT_CALL(*mock_zkclient, Die()).Times(1);
  EXPECT_CALL(*mock_zkclient_factory, Die()).Times(1);

  delete heron_zkstatemgr;
}

TEST_F(HeronZKStateMgrTest, testPackingPlanWatch) {
  const std::string topology_name = "dummy_topology";
  const std::string expected_path = topleveldir + "/packingplans/" + topology_name;

  HeronZKStateMgr* heron_zkstatemgr =
      new HeronZKStateMgrWithMock(hostportlist, topleveldir, ss, mock_zkclient_factory);

  heron_zkstatemgr->SetPackingPlanWatch(topology_name, []() { PackingPlanWatchHandler(); });

  // ensure TmanagerLocationWatch resets the watch
  EXPECT_CALL(*mock_zkclient, Exists(expected_path, _, _)).Times(1);

  packing_plan_watch_handler_count = 0;
  CallPackingPlanWatch(heron_zkstatemgr);
  // ensure watch handler is called.
  ASSERT_EQ(packing_plan_watch_handler_count, 1);

  EXPECT_CALL(*mock_zkclient, Die()).Times(1);
  EXPECT_CALL(*mock_zkclient_factory, Die()).Times(1);

  delete heron_zkstatemgr;
}

TEST_F(HeronZKStateMgrTest, testGlobalWatchEventHandler) {
  const std::string topology_name = "dummy_topology";
  const std::string expected_path = topleveldir + "/tmanagers/" + topology_name;

  heron::common::HeronZKStateMgr* heron_zkstatemgr =
      new HeronZKStateMgrWithMock(hostportlist, topleveldir, ss, mock_zkclient_factory);

  heron_zkstatemgr->SetTManagerLocationWatch(topology_name, []() { TmanagerLocationWatchHandler(); });

  tmanager_watch_handler_count = 0;
  const ZKClient::ZkWatchEvent session_expired_event = {ZOO_SESSION_EVENT,
                                                        ZOO_EXPIRED_SESSION_STATE, ""};

  // Ensure current ZkClient is destroyed
  EXPECT_CALL(*mock_zkclient, Die()).Times(1);
  // Ensure new ZkClient is created
  EXPECT_CALL(*mock_zkclient_factory, create(hostportlist, ss, _)).Times(1);

  CallGlobalWatchEventHandler(heron_zkstatemgr, session_expired_event);

  // Ensure watch handler is called
  ASSERT_EQ(tmanager_watch_handler_count, 1);

  EXPECT_CALL(*mock_zkclient_factory, Die()).Times(1);
  EXPECT_CALL(*mock_zkclient, Die()).Times(1);

  delete heron_zkstatemgr;
}
}  // namespace common
}  // namespace heron

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
