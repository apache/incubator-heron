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
#include <string>
#include <sstream>
#include <thread>
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
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "config/physical-plan-helper.h"
#include "manager/stateful-checkpointer.h"

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
    sp_string compname = "Spout";
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::OutputStream* ostream = spout->add_outputs();
    heron::proto::api::StreamId* tstream = ostream->mutable_stream();
    sp_string streamid = "streamid";
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
    sp_string compname = "bolt";
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::InputStream* istream = bolt->add_inputs();
    heron::proto::api::StreamId* tstream = istream->mutable_stream();
    sp_string streamid = "streamid";
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    sp_string input_compname = "spout";
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
  // Make topology stateful
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

heron::proto::system::Instance* CreateInstance(int32_t _comp, int32_t _comp_instance,
                                               int32_t _stmgr_id,
                                               int32_t _global_index, bool _is_spout) {
  heron::proto::system::Instance* imap = new heron::proto::system::Instance();
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

heron::proto::system::PhysicalPlan* CreatePplan() {
  int32_t nContainers = 2;
  int32_t nSpouts = 2;
  int32_t nSpoutInstances = 5;
  int32_t nBolts = 4;
  int32_t nBoltInstances = 10;
  auto topology = GenerateDummyTopology("TestTopology", "TestTopology-12345",
                                        nSpouts, nSpoutInstances, nBolts, nBoltInstances,
                                        heron::proto::api::SHUFFLE);
  auto pplan = new heron::proto::system::PhysicalPlan();
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
  int32_t global_index = 0;
  for (int spout = 0; spout < nSpouts; ++spout) {
    for (int spout_instance = 0; spout_instance < nSpoutInstances; ++spout_instance) {
      heron::proto::system::Instance* instance =
          CreateInstance(spout, spout_instance, stmgr_assignment, global_index++, true);
      if (++stmgr_assignment >= nContainers) {
        stmgr_assignment = 0;
      }
      pplan->add_instances()->CopyFrom(*instance);
      delete instance;
    }
  }
  for (int bolt = 0; bolt < nBolts; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < nBoltInstances; ++bolt_instance) {
      heron::proto::system::Instance* instance =
          CreateInstance(bolt, bolt_instance, stmgr_assignment, global_index++, false);
      if (++stmgr_assignment >= nContainers) {
        stmgr_assignment = 0;
      }
      pplan->add_instances()->CopyFrom(*instance);
      delete instance;
    }
  }
  return pplan;
}

// Test to make sure that we wait for all instances to be saved
// before declaring a checkpoint globally consistent
TEST(StatefulCheckpointer, test_stored_logic) {
  auto pplan = CreatePplan();
  auto checkpointer =
    new heron::tmanager::StatefulCheckpointer(std::chrono::high_resolution_clock::now());
  checkpointer->RegisterNewPhysicalPlan(*pplan);
  int32_t allButOne = pplan->instances_size() - 1;
  for (int32_t i = 0; i < allButOne; ++i) {
    ASSERT_FALSE(checkpointer->HandleInstanceStateStored("ckpt-1", pplan->instances(i)));
  }
  ASSERT_TRUE(checkpointer->HandleInstanceStateStored("ckpt-1", pplan->instances(allButOne)));
  delete pplan;
  delete checkpointer;
}

// Test to make sure that skipping to newer ckpt id behaves ok
TEST(StMgr, test_skip_to_newer_ckptid) {
  auto pplan = CreatePplan();
  auto checkpointer =
    new heron::tmanager::StatefulCheckpointer(std::chrono::high_resolution_clock::now());
  checkpointer->RegisterNewPhysicalPlan(*pplan);
  int32_t allButOne = pplan->instances_size() - 1;
  int32_t allButFive = pplan->instances_size() - 5;
  int32_t allButSeven = pplan->instances_size() - 7;
  for (int32_t i = 0; i < allButFive; ++i) {
    ASSERT_FALSE(checkpointer->HandleInstanceStateStored("ckpt-1", pplan->instances(i)));
  }
  // now move to newer ckpt
  for (int32_t i = 0; i < allButSeven; ++i) {
    ASSERT_FALSE(checkpointer->HandleInstanceStateStored("ckpt-2", pplan->instances(i)));
  }

  // Finish the older ones. Note that everything should return false
  for (int32_t i = allButFive; i < pplan->instances_size(); ++i) {
    ASSERT_FALSE(checkpointer->HandleInstanceStateStored("ckpt-1", pplan->instances(i)));
  }

  // Finish the newer ones
  for (int32_t i = allButSeven; i < allButOne; ++i) {
    ASSERT_FALSE(checkpointer->HandleInstanceStateStored("ckpt-2", pplan->instances(i)));
  }
  ASSERT_TRUE(checkpointer->HandleInstanceStateStored("ckpt-2", pplan->instances(allButOne)));

  delete pplan;
  delete checkpointer;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
