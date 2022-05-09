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

#include <limits>
#include <map>
#include <vector>
#include <sstream>
#include <thread>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include "gtest/gtest.h"
#include "glog/logging.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "config/physical-plan-helper.h"
#include "metrics/metrics-mgr-st.h"
#include "util/neighbour-calculator.h"
#include "manager/checkpoint-gateway.h"

const sp_string SPOUT_NAME = "spout";
const sp_string BOLT_NAME = "bolt";
const sp_string STREAM_NAME = "stream";
const sp_string CONTAINER_INDEX = "0";
const sp_string STMGR_NAME = "stmgr";
const sp_string LOCALHOST = "127.0.0.1";

// Generate a dummy topology
static heron::proto::api::Topology* GenerateDummyTopology(
    const sp_string& topology_name, const sp_string& topology_id, int num_spouts,
    int num_spout_instances, int num_bolts, int num_bolt_instances,
    const heron::proto::api::Grouping& grouping) {
  heron::proto::api::Topology* topology = new heron::proto::api::Topology();
  topology->set_id(topology_id);
  topology->set_name(topology_name);
  size_t spouts_size = num_spouts;
  size_t bolts_size = num_bolts;
  // Set spouts
  for (size_t i = 0; i < spouts_size; ++i) {
    heron::proto::api::Spout* spout = topology->add_spouts();
    // Set the component information
    heron::proto::api::Component* component = spout->mutable_comp();
    sp_string compname = SPOUT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::OutputStream* ostream = spout->add_outputs();
    heron::proto::api::StreamId* tstream = ostream->mutable_stream();
    sp_string streamid = STREAM_NAME;
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    tstream->set_component_name(compname);
    heron::proto::api::StreamSchema* schema = ostream->mutable_schema();
    heron::proto::api::StreamSchema::KeyType* key_type = schema->add_keys();
    key_type->set_key("dummy");
    key_type->set_type(heron::proto::api::OBJECT);
    // Set the config
    heron::proto::api::Config* config = component->mutable_config();
    heron::proto::api::Config::KeyValue* kv = config->add_kvs();
    kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
    kv->set_value(std::to_string(num_spout_instances));
  }
  // Set bolts
  for (size_t i = 0; i < bolts_size; ++i) {
    heron::proto::api::Bolt* bolt = topology->add_bolts();
    // Set the component information
    heron::proto::api::Component* component = bolt->mutable_comp();
    sp_string compname = BOLT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::InputStream* istream = bolt->add_inputs();
    heron::proto::api::StreamId* tstream = istream->mutable_stream();
    sp_string streamid = STREAM_NAME;
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    sp_string input_compname = SPOUT_NAME;
    input_compname += std::to_string(i);
    tstream->set_component_name(input_compname);
    istream->set_gtype(grouping);
    // Set the config
    heron::proto::api::Config* config = component->mutable_config();
    heron::proto::api::Config::KeyValue* kv = config->add_kvs();
    kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
    kv->set_value(std::to_string(num_bolt_instances));
  }
  // Set message timeout
  heron::proto::api::Config* topology_config = topology->mutable_topology_config();
  heron::proto::api::Config::KeyValue* kv = topology_config->add_kvs();
  kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_RELIABILITY_MODE);
  kv->set_value("ATLEAST_ONCE");

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

const sp_string CreateInstanceId(int32_t _global_index) {
  std::ostringstream instanceid_stream;
  instanceid_stream << "instance-" << _global_index;
  return instanceid_stream.str();
}

std::string GenerateStMgrId(int32_t _index) {
  std::ostringstream ostr;
  ostr << "stmgr-" << _index;
  return ostr.str();
}

unique_ptr<heron::proto::system::Instance> CreateInstance(int32_t _comp, int32_t _comp_instance,
                                               int32_t _stmgr_id,
                                               int32_t _global_index, bool _is_spout) {
  auto imap = make_unique<heron::proto::system::Instance>();
  imap->set_instance_id(CreateInstanceId(_global_index));
  imap->set_stmgr_id(GenerateStMgrId(_stmgr_id));
  heron::proto::system::InstanceInfo* inst = imap->mutable_info();
  inst->set_task_id(_global_index);
  inst->set_component_index(_comp_instance);
  if (_is_spout) {
    inst->set_component_name("spout" + std::to_string(_comp));
  } else {
    inst->set_component_name("bolt" + std::to_string(_comp));
  }
  return imap;
}

std::shared_ptr<heron::proto::system::PhysicalPlan> CreatePplan(int32_t _ncontainers,
                                                int32_t _nsbcomp, int32_t _ninstances) {
  int32_t nContainers = _ncontainers;
  int32_t nSpouts = _nsbcomp;
  int32_t nSpoutInstances = _ninstances;
  int32_t nBolts = _nsbcomp;
  int32_t nBoltInstances = _ninstances;
  auto topology = GenerateDummyTopology("TestTopology", "TestTopology-12345",
                                        nSpouts, nSpoutInstances, nBolts, nBoltInstances,
                                        heron::proto::api::SHUFFLE);
  auto pplan = std::make_shared<heron::proto::system::PhysicalPlan>();
  pplan->mutable_topology()->CopyFrom(*topology);
  delete topology;
  for (int32_t i = 0; i < nContainers; ++i) {
    auto stmgr = pplan->add_stmgrs();
    stmgr->set_id(GenerateStMgrId(i));
    stmgr->set_host_name("127.0.0.1");
    stmgr->set_data_port(100);
    stmgr->set_local_endpoint("101");
  }
  int32_t stmgr_assignment = 0;
  int32_t global_index = 1;
  for (int spout = 0; spout < nSpouts; ++spout) {
    for (int spout_instance = 0; spout_instance < nSpoutInstances; ++spout_instance) {
      auto instance = CreateInstance(spout, spout_instance, stmgr_assignment, global_index++, true);
      if (++stmgr_assignment >= nContainers) {
        stmgr_assignment = 0;
      }
      pplan->add_instances()->CopyFrom(*instance);
    }
  }
  for (int bolt = 0; bolt < nBolts; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < nBoltInstances; ++bolt_instance) {
      auto instance = CreateInstance(bolt, bolt_instance, stmgr_assignment, global_index++, false);
      if (++stmgr_assignment >= nContainers) {
        stmgr_assignment = 0;
      }
      pplan->add_instances()->CopyFrom(*instance);
    }
  }
  return pplan;
}

static std::vector<sp_int32> drainer1_tuples;
static std::vector<sp_int32> drainer2_tuples;
static std::vector<sp_int32> drainer3_markers;

void drainer1(sp_int32 _task_id, heron::proto::system::HeronTupleSet2* _tup) {
  drainer1_tuples.push_back(_task_id);
  delete _tup;
}

void drainer2(heron::proto::stmgr::TupleStreamMessage* _tup) {
  drainer2_tuples.push_back(_tup->task_id());
  delete _tup;
}

void drainer3(sp_int32 _task_id,
        pool_unique_ptr<heron::proto::ckptmgr::InitiateStatefulCheckpoint> _ckpt) {
  drainer3_markers.push_back(_task_id);
}

// Test to make sure that without any checkpoint business, things work smoothly
TEST(CheckpointGateway, emptyckptid) {
  for (int i = 1; i < 4; ++i) {
    for (int j = 1; j < 4; ++j) {
      auto pplan = CreatePplan(2, i, j);
      auto neighbour_calculator = std::make_shared<heron::stmgr::NeighbourCalculator>();
      neighbour_calculator->Reconstruct(*pplan);
      auto dummyLoop = std::make_shared<EventLoopImpl>();
      auto dummy_metrics_client_ =
              std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
      dummy_metrics_client_->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
      auto gateway = std::make_shared<heron::stmgr::CheckpointGateway>(1024 * 1024,
                                                         neighbour_calculator,
                                                         dummy_metrics_client_,
                                                         drainer1, drainer2, drainer3);
      int nTuples = 100;
      for (auto i = 0; i < nTuples; ++i) {
        auto tup = new heron::proto::system::HeronTupleSet2();
        gateway->SendToInstance(i, tup);
      }
      EXPECT_EQ(nTuples, drainer1_tuples.size());
      for (auto i = 0; i < nTuples; ++i) {
        EXPECT_EQ(i, drainer1_tuples[i]);
      }
      EXPECT_EQ(0, drainer2_tuples.size());
      EXPECT_EQ(0, drainer3_markers.size());

      drainer1_tuples.clear();
      drainer2_tuples.clear();
      drainer3_markers.clear();
    }
  }
}

void computeTasks(const heron::proto::system::PhysicalPlan& _pplan,
                  std::shared_ptr<heron::stmgr::NeighbourCalculator> _neighbour_calculator,
                  std::unordered_set<sp_int32>& _local_tasks,
                  std::unordered_set<sp_int32>& _local_spouts,
                  std::unordered_set<sp_int32>& _local_bolts,
                  std::unordered_map<sp_int32, std::unordered_set<sp_int32>>& _upstream_map) {
  const std::string& stmgr = _pplan.stmgrs(0).id();
  heron::config::PhysicalPlanHelper::GetTasks(_pplan, stmgr, _local_tasks);
  heron::config::PhysicalPlanHelper::GetLocalSpouts(_pplan, stmgr, _local_spouts);
  for (auto task : _local_tasks) {
    if (_local_spouts.find(task) == _local_spouts.end()) {
      _local_bolts.insert(task);
    }
  }
  for (auto local_bolt : _local_bolts) {
    _upstream_map[local_bolt] = _neighbour_calculator->get_upstreamers(local_bolt);
  }
}

// Test to check if tuple draining and buffering happens properly
TEST(CheckpointGateway, normaloperation) {
  for (int i = 1; i < 4; ++i) {
    for (int j = 1; j < 4; ++j) {
      auto pplan = CreatePplan(2, i, j);
      auto neighbour_calculator = std::make_shared<heron::stmgr::NeighbourCalculator>();
      neighbour_calculator->Reconstruct(*pplan);
      auto dummyLoop = std::make_shared<EventLoopImpl>();
      auto dummy_metrics_client_ =
              std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
      dummy_metrics_client_->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
      auto gateway = std::make_shared<heron::stmgr::CheckpointGateway>(1024 * 1024,
                                                         neighbour_calculator,
                                                         dummy_metrics_client_,
                                                         drainer1, drainer2, drainer3);
      // We will pretend to be one of the stmgrs
      std::unordered_set<sp_int32> local_tasks;
      std::unordered_set<sp_int32> local_spouts;
      std::unordered_set<sp_int32> local_bolts;
      std::unordered_map<sp_int32, std::unordered_set<sp_int32>> upstream_map;
      computeTasks(*pplan, neighbour_calculator, local_tasks, local_spouts,
                   local_bolts, upstream_map);

      // Let's make sure that at first, things just pass thru
      for (auto local_bolt : local_bolts) {
        EXPECT_EQ(0, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(0, drainer3_markers.size());
        auto tup = new heron::proto::system::HeronTupleSet2();
        gateway->SendToInstance(local_bolt, tup);
        EXPECT_EQ(1, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(0, drainer3_markers.size());
        drainer1_tuples.clear();
      }

      // Now let;s issue a ckpt marker
      std::string ckpt = "0";
      for (auto local_bolt : local_bolts) {
        sp_int32 upstreamer = *(upstream_map[local_bolt].begin());
        upstream_map[local_bolt].erase(upstreamer);
        gateway->HandleUpstreamMarker(upstreamer, local_bolt, ckpt);
        // Now send another tuple from the upstreamer.
        auto tup = new heron::proto::system::HeronTupleSet2();
        tup->set_src_task_id(upstreamer);
        gateway->SendToInstance(local_bolt, tup);
        if (upstream_map[local_bolt].empty()) {
          // They only have one upstreamer, so the tuple is passed thru
          EXPECT_EQ(1, drainer1_tuples.size());
          EXPECT_EQ(0, drainer2_tuples.size());
          EXPECT_EQ(1, drainer3_markers.size());
        } else {
          // These should be buffered
          EXPECT_EQ(0, drainer1_tuples.size());
          EXPECT_EQ(0, drainer2_tuples.size());
          EXPECT_EQ(0, drainer3_markers.size());
        }
        // Send the rest of the checkpoint markers
        for (auto src : upstream_map[local_bolt]) {
          gateway->HandleUpstreamMarker(src, local_bolt, ckpt);
        }
        // Things should have been drained
        EXPECT_EQ(1, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(1, drainer3_markers.size());

        // Next tuples should be passed thru without blocking
        tup = new heron::proto::system::HeronTupleSet2();
        tup->set_src_task_id(upstreamer);
        gateway->SendToInstance(local_bolt, tup);
        EXPECT_EQ(2, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(1, drainer3_markers.size());
        drainer1_tuples.clear();
        drainer2_tuples.clear();
        drainer3_markers.clear();
      }

      // clean things up
      drainer1_tuples.clear();
      drainer2_tuples.clear();
      drainer3_markers.clear();
    }
  }
}

// Test to check if overflow works
TEST(CheckpointGateway, overflow) {
  for (int i = 1; i < 4; ++i) {
    for (int j = 1; j < 4; ++j) {
      auto pplan = CreatePplan(2, i, j);
      auto neighbour_calculator = std::make_shared<heron::stmgr::NeighbourCalculator>();
      neighbour_calculator->Reconstruct(*pplan);
      auto dummyLoop = std::make_shared<EventLoopImpl>();
      auto dummy_metrics_client_ =
              std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
      dummy_metrics_client_->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
      auto gateway = std::make_shared<heron::stmgr::CheckpointGateway>(1024 * 1024,
                                                         neighbour_calculator,
                                                         dummy_metrics_client_,
                                                         drainer1, drainer2, drainer3);
      // We will pretend to be one of the stmgrs
      std::unordered_set<sp_int32> local_tasks;
      std::unordered_set<sp_int32> local_spouts;
      std::unordered_set<sp_int32> local_bolts;
      std::unordered_map<sp_int32, std::unordered_set<sp_int32>> upstream_map;
      computeTasks(*pplan, neighbour_calculator, local_tasks, local_spouts,
                   local_bolts, upstream_map);

      // Let's make sure that at first, things just pass thru
      for (auto local_bolt : local_bolts) {
        EXPECT_EQ(0, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(0, drainer3_markers.size());
        auto tup = new heron::proto::system::HeronTupleSet2();
        gateway->SendToInstance(local_bolt, tup);
        EXPECT_EQ(1, drainer1_tuples.size());
        EXPECT_EQ(0, drainer2_tuples.size());
        EXPECT_EQ(0, drainer3_markers.size());
        drainer1_tuples.clear();
      }

      // Now let;s issue a ckpt marker
      std::string ckpt = "0";
      for (auto local_bolt : local_bolts) {
        sp_int32 upstreamer = *(upstream_map[local_bolt].begin());
        upstream_map[local_bolt].erase(upstreamer);
        gateway->HandleUpstreamMarker(upstreamer, local_bolt, ckpt);
        // Now send another tuple from the upstreamer.
        auto tup = new heron::proto::system::HeronTupleSet2();
        tup->set_src_task_id(upstreamer);
        sp_uint64 cached_size = tup->ByteSizeLong();
        gateway->SendToInstance(local_bolt, tup);
        if (upstream_map[local_bolt].empty()) {
          // They only have one upstreamer, so the tuple is passed thru
          EXPECT_EQ(1, drainer1_tuples.size());
          EXPECT_EQ(0, drainer2_tuples.size());
          EXPECT_EQ(1, drainer3_markers.size());
        } else {
          // These should be buffered
          EXPECT_EQ(0, drainer1_tuples.size());
          EXPECT_EQ(0, drainer2_tuples.size());
          EXPECT_EQ(0, drainer3_markers.size());
          // Bombard lots of tuples from upstreamer
          sp_uint32 total_sent = 1;
          while (cached_size <= 1024 * 1024) {
            EXPECT_EQ(0, drainer1_tuples.size());
            EXPECT_EQ(0, drainer2_tuples.size());
            EXPECT_EQ(0, drainer3_markers.size());
            tup = new heron::proto::system::HeronTupleSet2();
            tup->set_src_task_id(upstreamer);
            cached_size += tup->ByteSizeLong();
            total_sent++;
            gateway->SendToInstance(local_bolt, tup);
          }
          // Send one more to tip over
          tup = new heron::proto::system::HeronTupleSet2();
          tup->set_src_task_id(upstreamer);
          cached_size += tup->ByteSizeLong();
          total_sent++;
          gateway->SendToInstance(local_bolt, tup);
          EXPECT_EQ(total_sent, drainer1_tuples.size());
          EXPECT_EQ(0, drainer2_tuples.size());
          EXPECT_EQ(0, drainer3_markers.size());
        }

        drainer1_tuples.clear();
        drainer2_tuples.clear();
        drainer3_markers.clear();
      }

      // clean things up
      drainer1_tuples.clear();
      drainer2_tuples.clear();
      drainer3_markers.clear();
    }
  }
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
