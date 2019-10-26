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
#include <sstream>
#include <vector>
#include "grouping/grouping.h"
#include "grouping/fields-grouping.h"
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

// Test to make sure that a particular tuple maps
// to the same task id
TEST(FieldsGrouping, test_sametuple) {
  std::vector<sp_int32> task_ids;
  task_ids.push_back(0);
  task_ids.push_back(2);
  task_ids.push_back(4);
  task_ids.push_back(8);
  std::vector<sp_int32> unsorted_ids;
  unsorted_ids.push_back(4);
  unsorted_ids.push_back(2);
  unsorted_ids.push_back(8);
  unsorted_ids.push_back(0);

  heron::proto::api::InputStream is;
  heron::proto::api::StreamSchema* s = is.mutable_grouping_fields();
  heron::proto::api::StreamSchema::KeyType* kt = s->add_keys();
  kt->set_type(heron::proto::api::OBJECT);
  kt->set_key("field1");

  heron::stmgr::FieldsGrouping* g = new heron::stmgr::FieldsGrouping(is, *s, task_ids);
  heron::stmgr::FieldsGrouping* g_unsorted = new heron::stmgr::FieldsGrouping(is, *s, unsorted_ids);
  heron::proto::system::HeronDataTuple tuple;
  tuple.add_values("dummy");

  std::set<sp_int32> all_dests;
  for (sp_int32 i = 0; i < 1000; ++i) {
    std::vector<sp_int32> dests;
    g->GetListToSend(tuple, dests);
    std::vector<sp_int32> unsorted_dests;
    g_unsorted->GetListToSend(tuple, unsorted_dests);
    EXPECT_EQ(dests.size(), (sp_uint32)1);
    EXPECT_EQ(unsorted_dests.size(), (sp_uint32)1);
    EXPECT_EQ(dests[0], unsorted_dests[0]);
    all_dests.insert(dests.front());
  }

  EXPECT_EQ(all_dests.size(), (sp_uint32)1);

  delete g;
  delete g_unsorted;
}

// Test that only the relevant fields are hashed
TEST(FieldsGrouping, test_hashonlyrelavant) {
  std::vector<sp_int32> task_ids;
  for (sp_int32 i = 0; i < 100; ++i) {
    task_ids.push_back(i);
  }

  heron::proto::api::InputStream is;
  heron::proto::api::StreamSchema* s = is.mutable_grouping_fields();
  heron::proto::api::StreamSchema::KeyType* kt = s->add_keys();
  kt->set_type(heron::proto::api::OBJECT);
  kt->set_key("field1");

  heron::proto::api::StreamSchema schema;
  heron::proto::api::StreamSchema::KeyType* kt1 = schema.add_keys();
  kt1->set_type(heron::proto::api::OBJECT);
  kt1->set_key("field1");
  heron::proto::api::StreamSchema::KeyType* kt2 = schema.add_keys();
  kt2->set_type(heron::proto::api::OBJECT);
  kt2->set_key("field2");

  heron::stmgr::FieldsGrouping* g = new heron::stmgr::FieldsGrouping(is, schema, task_ids);

  std::set<sp_int32> all_dests;
  for (sp_int32 i = 0; i < 1000; ++i) {
    heron::proto::system::HeronDataTuple tuple;
    tuple.add_values("this matters");
    std::ostringstream o;
    o << "this doesnt " << i;
    tuple.add_values(o.str());

    std::vector<sp_int32> dest;
    g->GetListToSend(tuple, dest);
    EXPECT_EQ(dest.size(), (sp_uint32)1);
    all_dests.insert(dest.front());
  }
  EXPECT_EQ(all_dests.size(), (sp_uint32)1);

  all_dests.clear();
  for (sp_int32 i = 0; i < 10000; ++i) {
    heron::proto::system::HeronDataTuple tuple;
    std::ostringstream o;
    o << "this changes " << i;
    tuple.add_values(o.str());
    tuple.add_values("this doesnt");

    std::vector<sp_int32> dest;
    g->GetListToSend(tuple, dest);
    EXPECT_EQ(dest.size(), (sp_uint32)1);
    all_dests.insert(dest.front());
  }
  EXPECT_EQ(all_dests.size(), task_ids.size());

  delete g;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
