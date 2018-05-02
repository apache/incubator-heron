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
#include "util/rotating-map.h"

// Test the removes
TEST(RotatingMap, test_removes) {
  sp_int32 nbuckets = 3;
  heron::stmgr::RotatingMap* g = new heron::stmgr::RotatingMap(nbuckets);

  // Create some items
  for (sp_int32 i = 0; i < 100; ++i) {
    g->create(i, 1);
  }

  // Make sure that removes work
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->remove(i), true);
  }

  // Unknown items cant be removed
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->remove(i), false);
  }

  // Create some more
  for (sp_int32 i = 0; i < 100; ++i) {
    g->create(i, 1);
  }

  // Rotate once
  g->rotate();

  // Make sure that removes work
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->remove(i), true);
  }

  // Unknown items cant be removed
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->remove(i), false);
  }

  // Create some more
  for (sp_int32 i = 0; i < 100; ++i) {
    g->create(i, 1);
  }

  // Rotate nbuckets times
  for (sp_int32 i = 0; i < nbuckets; ++i) {
    g->rotate();
  }

  // removes dont work
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->remove(i), false);
  }

  delete g;
}

// Test the anchoring and xoring logic
TEST(RotatingMap, test_anchors) {
  sp_int32 nbuckets = 3;
  heron::stmgr::RotatingMap* g = new heron::stmgr::RotatingMap(nbuckets);

  // Create some items
  for (sp_int32 i = 0; i < 100; ++i) {
    g->create(i, 1);
  }

  // basic Anchor works
  for (sp_int32 i = 0; i < 100; ++i) {
    EXPECT_EQ(g->anchor(i, 1), true);
    EXPECT_EQ(g->remove(i), true);
  }

  // layered anchoring
  std::vector<sp_int64> things_added;
  sp_int64 first_key = RandUtils::lrand();
  g->create(1, first_key);
  things_added.push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    things_added.push_back(key);
    EXPECT_EQ(g->anchor(1, key), false);
  }

  // xor ing works
  for (sp_int32 j = 0; j < 99; ++j) {
    EXPECT_EQ(g->anchor(1, things_added[j]), false);
  }

  EXPECT_EQ(g->anchor(1, things_added[99]), true);
  EXPECT_EQ(g->remove(1), true);

  // Same test with some rotation
  things_added.clear();
  first_key = RandUtils::lrand();
  g->create(1, first_key);
  things_added.push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    things_added.push_back(key);
    EXPECT_EQ(g->anchor(1, key), false);
  }

  g->rotate();

  for (sp_int32 j = 0; j < 99; ++j) {
    EXPECT_EQ(g->anchor(1, things_added[j]), false);
  }

  EXPECT_EQ(g->anchor(1, things_added[99]), true);
  EXPECT_EQ(g->remove(1), true);

  // Too much rotation
  things_added.clear();
  first_key = RandUtils::lrand();
  g->create(1, first_key);
  things_added.push_back(first_key);
  for (sp_int32 j = 1; j < 100; ++j) {
    sp_int64 key = RandUtils::lrand();
    things_added.push_back(key);
    EXPECT_EQ(g->anchor(1, key), false);
  }

  for (sp_int32 i = 0; i < nbuckets; ++i) {
    g->rotate();
  }

  for (sp_int32 j = 0; j < 100; ++j) {
    EXPECT_EQ(g->anchor(1, things_added[j]), false);
  }

  EXPECT_EQ(g->remove(1), false);

  delete g;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
