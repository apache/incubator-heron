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

#include <list>
#include <set>
#include <vector>
#include "gtest/gtest.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

#include "grouping/grouping.h"
#include "grouping/shuffle-grouping.h"

// Test round robin nature of shuffle grouping
TEST(ShuffleGrouping, test_roundrobin) {
  std::vector<sp_int32> task_ids;
  task_ids.push_back(0);
  task_ids.push_back(2);
  task_ids.push_back(4);
  task_ids.push_back(8);

  heron::stmgr::ShuffleGrouping* g = new heron::stmgr::ShuffleGrouping(task_ids);
  heron::proto::system::HeronDataTuple dummy;
  std::vector<sp_int32> dest;
  g->GetListToSend(dummy, dest);
  EXPECT_EQ(dest.size(), (sp_uint32)1);
  sp_int32 first = dest.front();
  sp_int32 index = -1;
  for (sp_uint32 i = 0; i < task_ids.size(); ++i) {
    if (task_ids[i] == first) {
      index = i;
      break;
    }
  }
  dest.clear();

  for (sp_int32 i = 0; i < 100; ++i) {
    g->GetListToSend(dummy, dest);
    EXPECT_EQ(dest.size(), (sp_uint32)1);
    sp_int32 d = dest.front();
    index = (index + 1) % task_ids.size();
    EXPECT_EQ(d, task_ids[index]);
    dest.clear();
  }

  delete g;
}

// Test random start
TEST(ShuffleGrouping, test_randomstart) {
  std::vector<sp_int32> task_ids;
  task_ids.push_back(0);
  task_ids.push_back(1);
  sp_int32 zeros = 0;
  sp_int32 ones = 0;
  sp_int32 count = 1000;

  for (sp_int32 i = 0; i < count; ++i) {
    heron::stmgr::ShuffleGrouping* g = new heron::stmgr::ShuffleGrouping(task_ids);
    heron::proto::system::HeronDataTuple dummy;
    std::vector<sp_int32> dest;
    g->GetListToSend(dummy, dest);
    EXPECT_EQ(dest.size(), (sp_uint32)1);
    sp_int32 first = dest.front();
    if (first == 0) {
      zeros++;
    } else {
      ones++;
    }
    dest.clear();
    delete g;
  }

  sp_double64 variance = ((sp_double64)abs(zeros - ones)) / count;
  EXPECT_LT(variance, 0.1);
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
