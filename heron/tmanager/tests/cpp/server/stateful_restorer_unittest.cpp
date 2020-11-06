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

#include <map>
#include <sstream>
#include <thread>
#include <vector>
#include "server/dummystmgr.h"
#include "server/dummytmanager.h"
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
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "config/physical-plan-helper.h"
#include "manager/stateful-restorer.h"

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
  kv->set_value(std::to_string(heron::config::TopologyConfigVars::EFFECTIVELY_ONCE));

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

const sp_string CreateInstanceId(sp_int8 type, sp_int8 instance, bool spout) {
  std::ostringstream instanceid_stream;
  if (spout) {
    instanceid_stream << CONTAINER_INDEX << "_" << SPOUT_NAME << static_cast<int>(type);
  } else {
    instanceid_stream << CONTAINER_INDEX << "_" << BOLT_NAME << static_cast<int>(type);
  }
  instanceid_stream << "_" << static_cast<int>(instance);
  return instanceid_stream.str();
}

heron::proto::system::Instance* CreateInstanceMap(sp_int8 type, sp_int8 instance, sp_int32 stmgr_id,
                                                  sp_int32 global_index, bool spout) {
  heron::proto::system::Instance* imap = new heron::proto::system::Instance();
  imap->set_instance_id(CreateInstanceId(type, instance, spout));

  imap->set_stmgr_id(STMGR_NAME + std::to_string(stmgr_id));
  heron::proto::system::InstanceInfo* inst = imap->mutable_info();
  inst->set_task_id(global_index);
  inst->set_component_index(instance);
  if (spout) {
    inst->set_component_name(SPOUT_NAME + std::to_string(type));
  } else {
    inst->set_component_name(BOLT_NAME + std::to_string(type));
  }
  return imap;
}

// Dummy timer cb. See StartServer for explanation.
void DummyTimerCb(EventLoop::Status) {
  // Do nothing
}

// Function to start the threads
void StartServer(std::shared_ptr<EventLoopImpl> ss) {
  // In the ss register a dummy timer. This is to make sure that the
  // exit from the loop happens timely. If there are no timers registered
  // with the select server and also no activity on any of the fds registered
  // for read/write then after the loopExit the loop continues indefinitely
  ss->registerTimer([](EventLoop::Status status) { DummyTimerCb(status); }, true, 1 * 1000 * 1000);
  ss->loop();
}

void StartDummyTManager(std::shared_ptr<EventLoopImpl>& ss, heron::testing::DummyTManager*& mgr,
                       std::thread*& tmanager_thread, sp_int32 tmanager_port) {
  // Create the select server for this stmgr to use
  ss = std::make_shared<EventLoopImpl>();

  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(tmanager_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  mgr = new heron::testing::DummyTManager(ss, options);
  mgr->Start();
  tmanager_thread = new std::thread(StartServer, ss);
}

void StartDummyStMgr(std::shared_ptr<EventLoopImpl>& ss, heron::testing::DummyStMgr*& mgr,
                     std::thread*& stmgr_thread, const sp_string tmanager_host,
                     sp_int32 tmanager_port, const sp_string& stmgr_id, sp_int32 stmgr_port,
                     const std::vector<heron::proto::system::Instance*>& instances) {
  // Create the select server for this stmgr to use
  ss = std::make_shared<EventLoopImpl>();

  NetworkOptions options;
  options.set_host(tmanager_host);
  options.set_port(tmanager_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  mgr = new heron::testing::DummyStMgr(ss, options, stmgr_id, LOCALHOST, stmgr_port, instances);
  mgr->Start();
  stmgr_thread = new std::thread(StartServer, ss);
  // Start the stream manager
  // stmgr_thread->start();
}

struct CommonResources {
  // arguments
  sp_int32 tmanager_port_;
  sp_int32 stmgr_baseport_;
  sp_string topology_name_;
  sp_string topology_id_;
  sp_int32 num_stmgrs_;
  sp_int32 num_spouts_;
  sp_int32 num_spout_instances_;
  sp_int32 num_bolts_;
  sp_int32 num_bolt_instances_;

  heron::proto::api::Grouping grouping_;

  // returns - filled in by init
  std::vector<std::shared_ptr<EventLoopImpl>> ss_list_;
  std::vector<sp_string> stmgrs_id_list_;
  heron::proto::api::Topology* topology_;

  // Tmanager
  heron::testing::DummyTManager* tmanager_;
  std::thread* tmanager_thread_;
  Piper* tmanager_piper_;

  // Stmgr
  std::vector<heron::testing::DummyStMgr*> stmgrs_list_;
  std::vector<std::thread*> stmgrs_threads_list_;

  // Stmgr to instance ids
  std::map<sp_int32, std::vector<sp_string> > stmgr_instance_id_list_;

  // Stmgr to Instance
  std::map<sp_int32, std::vector<heron::proto::system::Instance*> > stmgr_instance_list_;

  // Instanceid to instance
  std::map<sp_string, heron::proto::system::Instance*> instanceid_instance_;

  std::map<sp_string, sp_int32> instanceid_stmgr_;
  CommonResources() : topology_(nullptr),
                      tmanager_(nullptr),
                      tmanager_thread_(nullptr),
                      tmanager_piper_(nullptr) {
  }
};

void StartDummyTManager(CommonResources& common) {
  // Generate a dummy topology
  common.topology_ = GenerateDummyTopology(
      common.topology_name_, common.topology_id_, common.num_spouts_, common.num_spout_instances_,
      common.num_bolts_,  common.num_bolt_instances_, common.grouping_);

  // Populate the list of stmgrs
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    sp_string id = STMGR_NAME;
    id += std::to_string(i);
    common.stmgrs_id_list_.push_back(id);
  }

  // Start the tmanager
  std::shared_ptr<EventLoopImpl> tmanager_eventloop = nullptr;

  StartDummyTManager(tmanager_eventloop, common.tmanager_, common.tmanager_thread_,
                    common.tmanager_port_);
  common.ss_list_.push_back(tmanager_eventloop);
  common.tmanager_piper_ = new Piper(tmanager_eventloop);
}

void DistributeWorkersAcrossStmgrs(CommonResources& common) {
  // which stmgr is this component going to get assigned to
  sp_int32 stmgr_assignment_round = 0;
  sp_int32 global_index = 0;
  // Distribute the spouts
  for (int spout = 0; spout < common.num_spouts_; ++spout) {
    for (int spout_instance = 0; spout_instance < common.num_spout_instances_; ++spout_instance) {
      heron::proto::system::Instance* imap =
          CreateInstanceMap(spout, spout_instance, stmgr_assignment_round, global_index++, true);
      common.stmgr_instance_id_list_[stmgr_assignment_round].push_back(imap->instance_id());
      common.stmgr_instance_list_[stmgr_assignment_round].push_back(imap);
      common.instanceid_instance_[imap->instance_id()] = imap;
      common.instanceid_stmgr_[imap->instance_id()] = stmgr_assignment_round;
      // Have we completed a round of distribution of components
      if (++stmgr_assignment_round == common.num_stmgrs_) {
        stmgr_assignment_round = 0;
      }
    }
  }

  stmgr_assignment_round = 0;

  // Distributed the bolts
  for (int bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      heron::proto::system::Instance* imap =
          CreateInstanceMap(bolt, bolt_instance, stmgr_assignment_round, global_index++, false);
      common.stmgr_instance_id_list_[stmgr_assignment_round].push_back(imap->instance_id());
      common.stmgr_instance_list_[stmgr_assignment_round].push_back(imap);
      common.instanceid_instance_[imap->instance_id()] = imap;
      common.instanceid_stmgr_[imap->instance_id()] = stmgr_assignment_round;
      // Have we completed a round of distribution of components
      if (++stmgr_assignment_round == common.num_stmgrs_) {
        stmgr_assignment_round = 0;
      }
    }
  }
}

void StartStMgrs(CommonResources& common) {
  // Spwan and start the stmgrs
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    std::shared_ptr<EventLoopImpl> stmgr_ss = nullptr;
    heron::testing::DummyStMgr* mgr = nullptr;
    std::thread* stmgr_thread = nullptr;
    StartDummyStMgr(stmgr_ss, mgr, stmgr_thread, LOCALHOST, common.tmanager_port_,
                    common.stmgrs_id_list_[i], common.stmgr_baseport_ + i,
                    common.stmgr_instance_list_[i]);

    common.ss_list_.push_back(stmgr_ss);
    common.stmgrs_list_.push_back(mgr);
    common.stmgrs_threads_list_.push_back(stmgr_thread);
  }
}


void SetUpCommonResources(CommonResources& common) {
  // Initialize dummy params
  common.tmanager_port_ = 53001;
  common.stmgr_baseport_ = 53002;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 5;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 5;
  common.num_bolt_instances_ = 2;
  common.grouping_ = heron::proto::api::SHUFFLE;
}

void TearCommonResources(CommonResources& common) {
  delete common.topology_;
  delete common.tmanager_;
  delete common.tmanager_thread_;
  delete common.tmanager_piper_;

  // Cleanup the stream managers
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    delete common.stmgrs_list_[i];
    delete common.stmgrs_threads_list_[i];
  }

  common.ss_list_.clear();

  for (auto itr : common.instanceid_instance_) {
    delete itr.second;
  }
}

// Test to make sure that the restorer sends restore request
// and sends it to all stmgrs
TEST(StatefulRestorer, test_restore_send) {
  CommonResources common;
  SetUpCommonResources(common);

  // Start the tmanager etc.
  StartDummyTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stmgr
  StartStMgrs(common);

  // Wait until all stmgrs registered
  while (common.tmanager_->stmgrs().size() != common.num_stmgrs_) sleep(1);

  // Make sure that stmgrs have not gotten any restore message
  for (auto stmgr : common.stmgrs_list_) {
    EXPECT_FALSE(stmgr->GotRestoreMessage());
  }
  // Start Restorer
  auto restorer = new heron::tmanager::StatefulRestorer();
  EXPECT_FALSE(restorer->IsInProgress());
  common.tmanager_piper_->ExecuteInEventLoop(
        std::bind(&heron::tmanager::StatefulRestorer::StartRestore,
                  restorer, "ckpt-1", common.tmanager_->stmgrs()));

  sleep(1);
  // all stmgrs should have received restore message
  for (auto stmgr : common.stmgrs_list_) {
    EXPECT_TRUE(stmgr->GotRestoreMessage());
  }
  EXPECT_TRUE(restorer->IsInProgress());
  sp_int64 txid = restorer->GetRestoreTxid();

  // Simulate restored message
  for (auto stmgr : common.stmgrs_list_) {
    EXPECT_FALSE(restorer->GotResponse(stmgr->stmgrid()));
    EXPECT_FALSE(stmgr->GotStartProcessingMessage());
    common.tmanager_piper_->ExecuteInEventLoop(
        std::bind(&heron::tmanager::StatefulRestorer::HandleStMgrRestored,
                  restorer, stmgr->stmgrid(), "ckpt-1", txid, common.tmanager_->stmgrs()));
    sleep(1);
    EXPECT_TRUE(restorer->GotResponse(stmgr->stmgrid()));
  }

  // The 2 phase commit should have finished
  EXPECT_FALSE(restorer->IsInProgress());

  sleep(1);

  // All stmgrs should have gotten start message
  for (auto stmgr : common.stmgrs_list_) {
    EXPECT_TRUE(stmgr->GotStartProcessingMessage());
  }

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  // Delete the common resources
  TearCommonResources(common);
  delete restorer;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
