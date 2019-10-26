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

#include <algorithm>
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
#include "grouping/all-grouping.h"

// Test to make sure that all task ids are returned
TEST(AllGrouping, test_allgrouping) {
  std::vector<sp_int32> task_ids;
  for (sp_int32 i = 0; i < 100; ++i) {
    task_ids.push_back(i);
  }

  heron::stmgr::AllGrouping* g = new heron::stmgr::AllGrouping(task_ids);
  for (sp_int32 i = 0; i < 1000; ++i) {
    heron::proto::system::HeronDataTuple dummy;
    std::vector<sp_int32> dest;
    g->GetListToSend(dummy, dest);
    EXPECT_EQ(dest.size(), task_ids.size());
    std::sort(dest.begin(), dest.end());

    for (sp_int32 j = task_ids.size() - 1; j >= 0; --j) {
      EXPECT_EQ(task_ids[j], dest.back());
      dest.pop_back();
    }
  }

  delete g;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
