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
#include "config/heron-internals-config-reader.h"
#include "metrics/metrics-mgr-st.h"
#include "manager/stateful-restorer.h"
#include "server/dummy_ckptmgr_client.h"
#include "server/dummy_tuple_cache.h"
#include "server/dummy_stmgr_clientmgr.h"
#include "server/dummy_instance_server.h"

const sp_string SPOUT_NAME = "spout";
const sp_string BOLT_NAME = "bolt";
const sp_string STREAM_NAME = "stream";
const sp_string CONTAINER_INDEX = "0";
const sp_string STMGR_NAME = "stmgr";
const sp_string LOCALHOST = "127.0.0.1";

sp_string heron_internals_config_filename =
    "../../../../../../../../heron/config/heron_internals.yaml";

using std::shared_ptr;

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

heron::proto::system::PhysicalPlan* CreatePplan(int32_t _ncontainers,
                                                int32_t _nsbcomp, int32_t _ninstances) {
  int32_t nContainers = _ncontainers;
  int32_t nSpouts = _nsbcomp;
  int32_t nSpoutInstances = _ninstances;
  int32_t nBolts = _nsbcomp;
  int32_t nBoltInstances = _ninstances;
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
  int32_t global_index = 1;
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

shared_ptr<DummyCkptMgrClient> CreateDummyCkptMgr(heron::proto::system::PhysicalPlan* _pplan,
                                       std::shared_ptr<EventLoop> _eventLoop) {
  NetworkOptions options;
  return std::make_shared<DummyCkptMgrClient>(_eventLoop, options, GenerateStMgrId(1), _pplan);
}

shared_ptr<DummyInstanceServer> CreateDummyInstanceServer(std::shared_ptr<EventLoop> _eventLoop,
                                 const std::string& _stmgr,
                                 heron::proto::system::PhysicalPlan* _pplan,
                                 std::shared_ptr<heron::common::MetricsMgrSt> const& _metrics) {
NetworkOptions options;
  std::vector<std::string> dummy_instances;
  return std::make_shared<DummyInstanceServer>(_eventLoop, options, _stmgr, _pplan, dummy_instances,
                                 _metrics);
}

void RestoreDone(bool* _restore_done) {
  *_restore_done = true;
}

// Normal case. Start Restore, get ckpts from all, send restores, get restores from all
// and then the done watcher called
TEST(StatefulRestorer, normalcase) {
  auto pplan = CreatePplan(2, 4, 3);
  auto dummyLoop = std::make_shared<EventLoopImpl>();
  auto ckptmgr_client = CreateDummyCkptMgr(pplan, dummyLoop);
  auto tuple_cache = std::make_shared<DummyTupleCache>(dummyLoop);
  auto dummy_metrics_client = std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
  dummy_metrics_client->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
  auto dummy_stmgr_clientmgr = std::make_shared<DummyStMgrClientMgr>(dummyLoop,
                                                       dummy_metrics_client,
                                                       GenerateStMgrId(1), pplan);
  auto dummy_instance_server = CreateDummyInstanceServer(dummyLoop, GenerateStMgrId(1),
                                                      pplan, dummy_metrics_client);
  dummy_instance_server->Start();

  bool restore_done = false;
  std::string ckpt_id = "ckpt1";
  auto restorer = new heron::stmgr::StatefulRestorer(ckptmgr_client, dummy_stmgr_clientmgr,
                                                     tuple_cache, dummy_instance_server,
                                                     dummy_metrics_client,
                                                     std::bind(&RestoreDone, &restore_done));
  // At the start the restore is not in progress
  EXPECT_FALSE(restorer->InProgress());
  std::unordered_set<sp_int32> local_taskids;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_taskids);
  restorer->StartRestore(ckpt_id, 1, local_taskids, *pplan);
  // Now we are in restore
  EXPECT_TRUE(restorer->InProgress());
  // Make sure that the ckpt messages were sent to our tasks
  std::unordered_set<int32_t> local_tasks;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_tasks);
  for (auto task : local_tasks) {
    EXPECT_TRUE(ckptmgr_client->GetCalled(ckpt_id, task));
  }
  local_tasks.clear();
  // Make sure that the ckpt messages were not sent to some other tasks
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(0), local_tasks);
  for (auto task : local_tasks) {
    EXPECT_FALSE(ckptmgr_client->GetCalled(ckpt_id, task));
  }
  local_tasks.clear();

  // Responses from ckptmgr
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_tasks);
  for (auto task : local_tasks) {
    EXPECT_FALSE(dummy_instance_server->DidSendRestoreRequest(task));
    heron::proto::ckptmgr::InstanceStateCheckpoint c;
    c.set_checkpoint_id(ckpt_id);
    restorer->HandleCheckpointState(heron::proto::system::OK, task, ckpt_id, c);
    // Make sure that restore is sent to the instances
    EXPECT_TRUE(dummy_instance_server->DidSendRestoreRequest(task));
  }
  EXPECT_TRUE(restorer->InProgress());

  // Send notification that tasks have recovered
  for (auto task : local_tasks) {
    restorer->HandleInstanceRestoredState(task, heron::proto::system::OK, ckpt_id);
  }
  EXPECT_TRUE(restorer->InProgress());

  restorer->HandleAllStMgrClientsConnected();
  EXPECT_TRUE(restorer->InProgress());
  restorer->HandleAllInstancesConnected();
  EXPECT_FALSE(restorer->InProgress());
  EXPECT_TRUE(restore_done);

  delete restorer;
  delete pplan;
}

// If instances die in the middle of a restore, make sure that
// they have to be recovered again
TEST(StatefulRestorer, deadinstances) {
  auto pplan = CreatePplan(2, 4, 3);
  auto dummyLoop = std::make_shared<EventLoopImpl>();
  auto ckptmgr_client = CreateDummyCkptMgr(pplan, dummyLoop);
  auto tuple_cache = std::make_shared<DummyTupleCache>(dummyLoop);
  auto dummy_metrics_client = std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
  dummy_metrics_client->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
  auto dummy_stmgr_clientmgr = std::make_shared<DummyStMgrClientMgr>(dummyLoop,
                                                       dummy_metrics_client,
                                                       GenerateStMgrId(1), pplan);
  auto dummy_instance_server = CreateDummyInstanceServer(dummyLoop, GenerateStMgrId(1),
                                                         pplan, dummy_metrics_client);
  dummy_instance_server->Start();

  bool restore_done = false;
  std::string ckpt_id = "ckpt1";
  auto restorer = new heron::stmgr::StatefulRestorer(ckptmgr_client, dummy_stmgr_clientmgr,
                                                     tuple_cache, dummy_instance_server,
                                                     dummy_metrics_client,
                                                     std::bind(&RestoreDone, &restore_done));
  // At the start the restore is not in progress
  EXPECT_FALSE(restorer->InProgress());
  std::unordered_set<sp_int32> local_taskids;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_taskids);
  restorer->StartRestore(ckpt_id, 1, local_taskids, *pplan);
  // Now we are in restore
  EXPECT_TRUE(restorer->InProgress());
  // Just have all stmgrs connected to us
  restorer->HandleAllStMgrClientsConnected();

  // Responses from ckptmgr
  std::unordered_set<int32_t> local_tasks;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_tasks);
  for (auto task : local_tasks) {
    heron::proto::ckptmgr::InstanceStateCheckpoint c;
    c.set_checkpoint_id(ckpt_id);
    restorer->HandleCheckpointState(heron::proto::system::OK, task, ckpt_id, c);
  }
  EXPECT_TRUE(restorer->InProgress());

  // Send notification that some tasks have recovered
  EXPECT_GT(local_tasks.size(), 1);
  bool first = true;
  int32_t troublesome_task = 0;
  for (auto task : local_tasks) {
    if (first) {
      first = false;
      troublesome_task = task;
    } else {
      restorer->HandleInstanceRestoredState(task, heron::proto::system::OK, ckpt_id);
    }
  }
  EXPECT_TRUE(restorer->InProgress());

  // Notify that one instance is dead
  restorer->HandleDeadInstanceConnection(troublesome_task);
  dummy_instance_server->ClearSendRestoreRequest(troublesome_task);
  ckptmgr_client->ClearGetCalled(ckpt_id, troublesome_task);

  EXPECT_TRUE(restorer->InProgress());
  EXPECT_FALSE(ckptmgr_client->GetCalled(ckpt_id, troublesome_task));
  EXPECT_FALSE(dummy_instance_server->DidSendRestoreRequest(troublesome_task));

  // Now it is back on again
  restorer->HandleAllInstancesConnected();
  // We are still in progress
  EXPECT_TRUE(restorer->InProgress());
  // Make sure that ckpt request is sent again
  EXPECT_TRUE(ckptmgr_client->GetCalled(ckpt_id, troublesome_task));
  // The state is fetched from ckptmgr
  heron::proto::ckptmgr::InstanceStateCheckpoint c;
  c.set_checkpoint_id(ckpt_id);
  restorer->HandleCheckpointState(heron::proto::system::OK, troublesome_task, ckpt_id, c);
  EXPECT_TRUE(restorer->InProgress());
  // make sure that we sent restore state
  EXPECT_TRUE(dummy_instance_server->DidSendRestoreRequest(troublesome_task));
  restorer->HandleInstanceRestoredState(troublesome_task, heron::proto::system::OK, ckpt_id);

  // Now everything is done
  EXPECT_FALSE(restorer->InProgress());
  EXPECT_TRUE(restore_done);

  delete restorer;
  delete pplan;
}

// This tests the scenario where ckptmgr
// dies in the middle of a restore
TEST(StatefulRestorer, deadckptmgr) {
  auto pplan = CreatePplan(2, 4, 3);
  auto dummyLoop = std::make_shared<EventLoopImpl>();
  auto ckptmgr_client = CreateDummyCkptMgr(pplan, dummyLoop);
  auto tuple_cache = std::make_shared<DummyTupleCache>(dummyLoop);
  auto dummy_metrics_client = std::make_shared<heron::common::MetricsMgrSt>(11001, 100, dummyLoop);
  dummy_metrics_client->Start("127.0.0.1", 11000, "_stmgr", "_stmgr");
  auto dummy_stmgr_clientmgr = std::make_shared<DummyStMgrClientMgr>(dummyLoop,
                                                       dummy_metrics_client,
                                                       GenerateStMgrId(1), pplan);
  dummy_stmgr_clientmgr->SetAllStMgrClientsRegistered(true);
  auto dummy_instance_server = CreateDummyInstanceServer(dummyLoop, GenerateStMgrId(1),
                                                         pplan, dummy_metrics_client);
  dummy_instance_server->Start();
  dummy_instance_server->SetAllInstancesConnectedToUs(true);
  bool restore_done = false;
  std::string ckpt_id = "ckpt1";
  auto restorer = new heron::stmgr::StatefulRestorer(ckptmgr_client, dummy_stmgr_clientmgr,
                                                     tuple_cache, dummy_instance_server,
                                                     dummy_metrics_client,
                                                     std::bind(&RestoreDone, &restore_done));
  // At the start the restore is not in progress
  EXPECT_FALSE(restorer->InProgress());
  std::unordered_set<sp_int32> local_taskids;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_taskids);
  restorer->StartRestore(ckpt_id, 1, local_taskids, *pplan);
  // Now we are in restore
  EXPECT_TRUE(restorer->InProgress());

  // Responses from ckptmgr
  std::unordered_set<int32_t> local_tasks;
  heron::config::PhysicalPlanHelper::GetTasks(*pplan, GenerateStMgrId(1), local_tasks);
  // Send notification that some tasks have recovered
  EXPECT_GT(local_tasks.size(), 1);
  bool first = true;
  int32_t troublesome_task = 0;
  // ckpt delivers some checkpoints
  for (auto task : local_tasks) {
    if (first) {
      first = false;
      troublesome_task = task;
    } else {
      heron::proto::ckptmgr::InstanceStateCheckpoint c;
      c.set_checkpoint_id(ckpt_id);
      restorer->HandleCheckpointState(heron::proto::system::OK, task, ckpt_id, c);
      restorer->HandleInstanceRestoredState(task, heron::proto::system::OK, ckpt_id);
    }
  }
  EXPECT_TRUE(restorer->InProgress());

  dummy_instance_server->ClearSendRestoreRequest(troublesome_task);
  ckptmgr_client->ClearGetCalled(ckpt_id, troublesome_task);
  EXPECT_FALSE(ckptmgr_client->GetCalled(ckpt_id, troublesome_task));

  // Now ckpt mgr dies and comes back
  restorer->HandleCkptMgrRestart();

  // make sure that we sent the recover ckpt request
  EXPECT_TRUE(ckptmgr_client->GetCalled(ckpt_id, troublesome_task));
  EXPECT_TRUE(restorer->InProgress());
  restorer->HandleInstanceRestoredState(troublesome_task, heron::proto::system::OK, ckpt_id);

  // Now everything is done
  EXPECT_FALSE(restorer->InProgress());
  EXPECT_TRUE(restore_done);

  delete restorer;
  delete pplan;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }
  // Create the sington for heron_internals_config_reader, if it does not exist
  if (!heron::config::HeronInternalsConfigReader::Exists()) {
    heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename, "");
  }
  return RUN_ALL_TESTS();
}
