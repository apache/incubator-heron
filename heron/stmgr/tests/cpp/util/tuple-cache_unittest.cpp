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
#include <map>
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
#include "config/heron-internals-config-reader.h"
#include "util/tuple-cache.h"

sp_string heron_internals_config_filename =
    "../../../../../../../../heron/config/heron_internals.yaml";

class Drainer {
 public:
  Drainer(const std::map<sp_int32, sp_int32>& _num_data_tuples_expected,
          const std::map<sp_int32, sp_int32>& _num_ack_tuples_expected,
          const std::map<sp_int32, sp_int32>& _num_fail_tuples_expected)
      : num_data_tuples_expected_(_num_data_tuples_expected),
        num_ack_tuples_expected_(_num_ack_tuples_expected),
        num_fail_tuples_expected_(_num_fail_tuples_expected),
        ckpt_message_seen_(false) {}

  ~Drainer() {}

  void Drain(sp_int32 _task_id, heron::proto::system::HeronTupleSet2* _t) {
    if (_t->has_data()) {
      EXPECT_EQ(_t->has_control(), false);
      add_actual(_task_id, _t->data().tuples_size(), num_data_tuples_actual_);
    } else {
      EXPECT_EQ(_t->has_data(), false);
      add_actual(_task_id, _t->control().acks_size(), num_ack_tuples_actual_);
      add_actual(_task_id, _t->control().fails_size(), num_fail_tuples_actual_);
    }
    delete _t;
  }

  void CheckpointDrain(sp_int32 _task_id, heron::proto::ckptmgr::DownstreamStatefulCheckpoint* _t) {
    ckpt_message_seen_ = true;
  }

  bool Verify(bool _ckpt) {
    return verify(num_data_tuples_expected_, num_data_tuples_actual_) &&
           verify(num_ack_tuples_expected_, num_ack_tuples_actual_) &&
           verify(num_fail_tuples_expected_, num_fail_tuples_actual_) &&
           ckpt_message_seen_ == _ckpt;
  }

 private:
  void add_actual(sp_int32 _task_id, sp_int32 _increment,
                  std::map<sp_int32, sp_int32>& _which_map) {
    if (_which_map.find(_task_id) == _which_map.end()) {
      _which_map[_task_id] = _increment;
    } else {
      _which_map[_task_id] += _increment;
    }
  }

  bool verify(const std::map<sp_int32, sp_int32>& _first,
              const std::map<sp_int32, sp_int32>& _second) {
    if (_first.size() != _second.size()) return false;
    for (std::map<sp_int32, sp_int32>::const_iterator iter = _first.begin(); iter != _first.end();
         ++iter) {
      if (_second.find(iter->first) == _second.end()) return false;
      if (_second.find(iter->first)->second != iter->second) return false;
    }
    return true;
  }

  std::map<sp_int32, sp_int32> num_data_tuples_expected_;
  std::map<sp_int32, sp_int32> num_ack_tuples_expected_;
  std::map<sp_int32, sp_int32> num_fail_tuples_expected_;
  std::map<sp_int32, sp_int32> num_data_tuples_actual_;
  std::map<sp_int32, sp_int32> num_ack_tuples_actual_;
  std::map<sp_int32, sp_int32> num_fail_tuples_actual_;
  bool ckpt_message_seen_;
};

void DoneHandler(std::shared_ptr<EventLoopImpl> _ss, EventLoopImpl::Status) { _ss->loopExit(); }

// Test simple data tuples drain
TEST(TupleCache, test_simple_data_drain) {
  sp_int32 data_tuples_count = 23354;
  auto ss = std::make_shared<EventLoopImpl>();
  sp_uint32 drain_threshold = 1024 * 1024;
  heron::stmgr::TupleCache* g = new heron::stmgr::TupleCache(ss, drain_threshold);
  std::map<sp_int32, sp_int32> data_tuples;
  data_tuples[1] = data_tuples_count;
  std::map<sp_int32, sp_int32> ack_tuples;
  std::map<sp_int32, sp_int32> fail_tuples;
  Drainer* drainer = new Drainer(data_tuples, ack_tuples, fail_tuples);
  g->RegisterDrainer(&Drainer::Drain, drainer);

  heron::proto::api::StreamId dummy;
  dummy.set_id("stream");
  dummy.set_component_name("comp");
  for (sp_int32 i = 0; i < data_tuples_count; ++i) {
    heron::proto::system::HeronDataTuple tuple;
    tuple.set_key(RandUtils::lrand());
    g->add_data_tuple(1, 1, dummy, &tuple);
  }

  // 300 milliseconds second
  auto cb = [&ss](EventLoopImpl::Status status) { DoneHandler(ss, status); };
  ss->registerTimer(std::move(cb), false, 300_ms);

  ss->loop();

  EXPECT_EQ(drainer->Verify(false), true);
  delete drainer;
  delete g;
}

// Test data/ack/fail mix
TEST(TupleCache, test_data_ack_fail_mix) {
  sp_int32 data_tuples_count = 23354;
  sp_int32 ack_tuples_count = 3543;
  sp_int32 fail_tuples_count = 6564;
  auto ss = std::make_shared<EventLoopImpl>();
  sp_uint32 drain_threshold = 1024 * 1024;
  heron::stmgr::TupleCache* g = new heron::stmgr::TupleCache(ss, drain_threshold);
  std::map<sp_int32, sp_int32> data_tuples;
  data_tuples[1] = data_tuples_count;
  std::map<sp_int32, sp_int32> ack_tuples;
  ack_tuples[1] = ack_tuples_count;
  std::map<sp_int32, sp_int32> fail_tuples;
  fail_tuples[1] = fail_tuples_count;
  Drainer* drainer = new Drainer(data_tuples, ack_tuples, fail_tuples);
  g->RegisterDrainer(&Drainer::Drain, drainer);

  heron::proto::api::StreamId dummy;
  dummy.set_id("stream");
  dummy.set_component_name("comp");
  sp_int32 max_count = std::max(std::max(data_tuples_count, ack_tuples_count), fail_tuples_count);
  for (sp_int32 i = 0; i < max_count; ++i) {
    if (i < data_tuples_count) {
      heron::proto::system::HeronDataTuple tuple;
      tuple.set_key(RandUtils::lrand());
      g->add_data_tuple(1, 1, dummy, &tuple);
    }
    if (i < ack_tuples_count) {
      heron::proto::system::AckTuple tuple;
      tuple.set_ackedtuple(RandUtils::lrand());
      g->add_ack_tuple(1, 1, tuple);
    }
    if (i < fail_tuples_count) {
      heron::proto::system::AckTuple tuple;
      tuple.set_ackedtuple(RandUtils::lrand());
      g->add_fail_tuple(1, 1, tuple);
    }
  }

  // 2000 milliseconds second
  auto cb = [&ss](EventLoopImpl::Status status) { DoneHandler(ss, status); };
  ss->registerTimer(std::move(cb), false, 2000000);

  ss->loop();

  EXPECT_EQ(drainer->Verify(false), true);
  delete drainer;
  delete g;
}

// Test different stream mix
TEST(TupleCache, test_different_stream_mix) {
  sp_int32 data_tuples_count = 23354;  // make sure this is even
  sp_int32 ack_tuples_count = 3544;    // make sure this is even
  sp_int32 fail_tuples_count = 6564;   // make sure this is even
  auto ss = std::make_shared<EventLoopImpl>();
  sp_uint32 drain_threshold = 1024 * 1024;
  heron::stmgr::TupleCache* g = new heron::stmgr::TupleCache(ss, drain_threshold);
  std::map<sp_int32, sp_int32> data_tuples;
  data_tuples[1] = data_tuples_count / 2;
  data_tuples[2] = data_tuples_count / 2;
  std::map<sp_int32, sp_int32> ack_tuples;
  ack_tuples[1] = ack_tuples_count / 2;
  ack_tuples[2] = ack_tuples_count / 2;
  std::map<sp_int32, sp_int32> fail_tuples;
  fail_tuples[1] = fail_tuples_count / 2;
  fail_tuples[2] = fail_tuples_count / 2;
  Drainer* drainer = new Drainer(data_tuples, ack_tuples, fail_tuples);
  g->RegisterDrainer(&Drainer::Drain, drainer);

  heron::proto::api::StreamId stream1;
  stream1.set_id("stream1");
  stream1.set_component_name("comp1");
  heron::proto::api::StreamId stream2;
  stream2.set_id("stream2");
  stream2.set_component_name("comp2");
  sp_int32 max_count = std::max(std::max(data_tuples_count, ack_tuples_count), fail_tuples_count);
  for (sp_int32 i = 0; i < max_count; ++i) {
    if (i < data_tuples_count) {
      heron::proto::system::HeronDataTuple tuple;
      tuple.set_key(RandUtils::lrand());
      if (i % 2 == 0) {
        g->add_data_tuple(1, 1, stream1, &tuple);
      } else {
        g->add_data_tuple(1, 2, stream2, &tuple);
      }
    }
    if (i < ack_tuples_count) {
      heron::proto::system::AckTuple tuple;
      tuple.set_ackedtuple(RandUtils::lrand());
      if (i % 2 == 0) {
        g->add_ack_tuple(1, 1, tuple);
      } else {
        g->add_ack_tuple(1, 2, tuple);
      }
    }
    if (i < fail_tuples_count) {
      heron::proto::system::AckTuple tuple;
      tuple.set_ackedtuple(RandUtils::lrand());
      if (i % 2 == 0) {
        g->add_fail_tuple(1, 1, tuple);
      } else {
        g->add_fail_tuple(1, 2, tuple);
      }
    }
  }

  // 1000 milliseconds second
  auto cb = [&ss](EventLoopImpl::Status status) { DoneHandler(ss, status); };
  ss->registerTimer(std::move(cb), false, 1000000);

  ss->loop();

  EXPECT_EQ(drainer->Verify(false), true);
  delete drainer;
  delete g;
}

// Test drain with checkpoint marker
TEST(TupleCache, test_checkpoint_drain) {
  sp_int32 data_tuples_count = 23354;
  auto ss = std::make_shared<EventLoopImpl>();
  sp_uint32 drain_threshold = 1024 * 1024;
  heron::stmgr::TupleCache* g = new heron::stmgr::TupleCache(ss, drain_threshold);
  std::map<sp_int32, sp_int32> data_tuples;
  data_tuples[1] = data_tuples_count;
  std::map<sp_int32, sp_int32> ack_tuples;
  std::map<sp_int32, sp_int32> fail_tuples;
  Drainer* drainer = new Drainer(data_tuples, ack_tuples, fail_tuples);
  g->RegisterDrainer(&Drainer::Drain, drainer);
  g->RegisterCheckpointDrainer(&Drainer::CheckpointDrain, drainer);

  heron::proto::api::StreamId dummy;
  dummy.set_id("stream");
  dummy.set_component_name("comp");
  for (sp_int32 i = 0; i < data_tuples_count/2; ++i) {
    heron::proto::system::HeronDataTuple tuple;
    tuple.set_key(RandUtils::lrand());
    g->add_data_tuple(1, 1, dummy, &tuple);
  }

  heron::proto::ckptmgr::DownstreamStatefulCheckpoint ckpt_message;
  g->add_checkpoint_tuple(1, &ckpt_message);

  for (sp_int32 i = 0; i < data_tuples_count/2; ++i) {
    heron::proto::system::HeronDataTuple tuple;
    tuple.set_key(RandUtils::lrand());
    g->add_data_tuple(1, 1, dummy, &tuple);
  }

  // 300 milliseconds second
  auto cb = [&ss](EventLoopImpl::Status status) { DoneHandler(ss, status); };
  ss->registerTimer(std::move(cb), false, 300_ms);

  ss->loop();

  EXPECT_EQ(drainer->Verify(true), true);
  delete drainer;
  delete g;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }

  char path1[1024];  // This is a buffer for the text
  getcwd(path1, 1024);
  std::cout << "Current working directory " << path1 << std::endl;

  // Create the sington for heron_internals_config_reader, if it does not exist
  if (!heron::config::HeronInternalsConfigReader::Exists()) {
    heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename, "");
  }
  return RUN_ALL_TESTS();
}
