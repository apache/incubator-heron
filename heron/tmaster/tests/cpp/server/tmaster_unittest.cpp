/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <map>
#include <sstream>
#include <thread>
#include <vector>
#include "server/dummystmgr.h"
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
#include "statemgr/heron-statemgr.h"
#include "statemgr/heron-localfilestatemgr.h"
#include "manager/tmaster.h"
#include "manager/stmgr.h"

const sp_string SPOUT_NAME = "spout";
const sp_string BOLT_NAME = "bolt";
const sp_string STREAM_NAME = "stream";
const sp_string CONTAINER_INDEX = "0";
const sp_string STMGR_NAME = "stmgr";
const sp_string MESSAGE_TIMEOUT = "30";  // seconds
const sp_string LOCALHOST = "127.0.0.1";
sp_string heron_internals_config_filename =
    "../../../../../../../../heron/config/heron_internals.yaml";
sp_string metrics_sinks_config_filename = "../../../../../../../../heron/config/metrics_sinks.yaml";

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
  kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS);
  kv->set_value(MESSAGE_TIMEOUT);

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

// Method to create the local zk state on the filesystem
const sp_string CreateLocalStateOnFS(heron::proto::api::Topology* topology) {
  EventLoopImpl ss;

  // Create a temporary directory to write out the state
  char dpath[255];
  snprintf(dpath, sizeof(dpath), "%s", "/tmp/XXXXXX");
  mkdtemp(dpath);

  // Write the dummy topology/tmaster location out to the local file system
  // via thestate mgr
  heron::common::HeronLocalFileStateMgr state_mgr(dpath, &ss);
  state_mgr.CreateTopology(*topology, NULL);

  // Return the root path
  return sp_string(dpath);
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
  if (spout)
    inst->set_component_name(SPOUT_NAME + std::to_string(type));
  else
    inst->set_component_name(BOLT_NAME + std::to_string(type));
  return imap;
}

// Dummy timer cb. See StartServer for explanation.
void DummyTimerCb(EventLoop::Status) {
  // Do nothing
}

// Function to start the threads
void StartServer(EventLoopImpl* ss) {
  // In the ss register a dummy timer. This is to make sure that the
  // exit from the loop happens timely. If there are no timers registered
  // with the select server and also no activity on any of the fds registered
  // for read/write then after the  loopExit the loop continues indefinetly.
  ss->registerTimer([](EventLoop::Status status) { DummyTimerCb(status); }, true, 1 * 1000 * 1000);
  ss->loop();
}

void StartTMaster(EventLoopImpl*& ss, heron::tmaster::TMaster*& tmaster,
                  std::thread*& tmaster_thread, const sp_string& zkhostportlist,
                  const sp_string& topology_name, const sp_string& topology_id,
                  const sp_string& dpath, const std::vector<sp_string>& stmgrs_id_list,
                  sp_int32 tmaster_port, sp_int32 tmaster_controller_port) {
  ss = new EventLoopImpl();
  tmaster =
      new heron::tmaster::TMaster(zkhostportlist, topology_name, topology_id, dpath, stmgrs_id_list,
                                  tmaster_controller_port, tmaster_port, tmaster_port + 2,
                                  tmaster_port + 3, metrics_sinks_config_filename, LOCALHOST, ss);
  tmaster_thread = new std::thread(StartServer, ss);
  // tmaster_thread->start();
}

void StartDummyStMgr(EventLoopImpl*& ss, heron::testing::DummyStMgr*& mgr,
                     std::thread*& stmgr_thread, const sp_string tmaster_host,
                     sp_int32 tmaster_port, const sp_string& stmgr_id, sp_int32 stmgr_port,
                     const std::vector<heron::proto::system::Instance*>& instances) {
  // Create the select server for this stmgr to use
  ss = new EventLoopImpl();

  NetworkOptions options;
  options.set_host(tmaster_host);
  options.set_port(tmaster_port);
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
  sp_int32 tmaster_port_;
  sp_int32 tmaster_controller_port_;
  sp_int32 stmgr_baseport_;
  sp_string zkhostportlist_;
  sp_string topology_name_;
  sp_string topology_id_;
  sp_int32 num_stmgrs_;
  sp_int32 num_spouts_;
  sp_int32 num_spout_instances_;
  sp_int32 num_bolts_;
  sp_int32 num_bolt_instances_;

  heron::proto::api::Grouping grouping_;

  // returns - filled in by init
  sp_string dpath_;
  std::vector<EventLoopImpl*> ss_list_;
  std::vector<sp_string> stmgrs_id_list_;
  heron::proto::api::Topology* topology_;

  heron::tmaster::TMaster* tmaster_;
  std::thread* tmaster_thread_;

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
  CommonResources() : topology_(NULL), tmaster_(NULL), tmaster_thread_(NULL) {
    // Create the sington for heron_internals_config_reader
    // if it does not exist
    if (!heron::config::HeronInternalsConfigReader::Exists()) {
      heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename);
    }
  }
};

void StartTMaster(CommonResources& common) {
  // Generate a dummy topology
  common.topology_ = GenerateDummyTopology(
      common.topology_name_, common.topology_id_, common.num_spouts_, common.num_spout_instances_,
      common.num_bolts_, common.num_bolt_instances_, common.grouping_);

  // Create the zk state on the local file system
  common.dpath_ = CreateLocalStateOnFS(common.topology_);

  // Poulate the list of stmgrs
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    sp_string id = STMGR_NAME;
    id += std::to_string(i);
    common.stmgrs_id_list_.push_back(id);
  }

  // Start the tmaster
  EventLoopImpl* tmaster_eventLoop;

  StartTMaster(tmaster_eventLoop, common.tmaster_, common.tmaster_thread_, common.zkhostportlist_,
               common.topology_name_, common.topology_id_, common.dpath_, common.stmgrs_id_list_,
               common.tmaster_port_, common.tmaster_controller_port_);
  common.ss_list_.push_back(tmaster_eventLoop);
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

  // Distribute the bolts
  for (int bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      heron::proto::system::Instance* imap =
          CreateInstanceMap(bolt, bolt_instance, stmgr_assignment_round, global_index++, false);
      // Have we completed a round of distribution of components
      common.stmgr_instance_id_list_[stmgr_assignment_round].push_back(imap->instance_id());
      common.stmgr_instance_list_[stmgr_assignment_round].push_back(imap);
      common.instanceid_instance_[imap->instance_id()] = imap;
      common.instanceid_stmgr_[imap->instance_id()] = stmgr_assignment_round;
      if (++stmgr_assignment_round == common.num_stmgrs_) {
        stmgr_assignment_round = 0;
      }
    }
  }
}

void StartStMgrs(CommonResources& common) {
  // Spawn and start the stmgrs
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    EventLoopImpl* stmgr_ss = NULL;
    heron::testing::DummyStMgr* mgr = NULL;
    std::thread* stmgr_thread = NULL;
    StartDummyStMgr(stmgr_ss, mgr, stmgr_thread, LOCALHOST, common.tmaster_port_,
                    common.stmgrs_id_list_[i], common.stmgr_baseport_ + i,
                    common.stmgr_instance_list_[i]);

    common.ss_list_.push_back(stmgr_ss);
    common.stmgrs_list_.push_back(mgr);
    common.stmgrs_threads_list_.push_back(stmgr_thread);
  }
}

void SetUpCommonResources(CommonResources& common) {
  // Initialize dummy params
  common.tmaster_port_ = 53001;
  common.tmaster_controller_port_ = 53002;
  common.stmgr_baseport_ = 53001;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 5;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 5;
  common.num_bolt_instances_ = 2;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";
}

void TearCommonResources(CommonResources& common) {
  delete common.topology_;
  delete common.tmaster_thread_;
  delete common.tmaster_;

  // Cleanup the stream managers
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    delete common.stmgrs_list_[i];
    delete common.stmgrs_threads_list_[i];
  }

  for (size_t i = 0; i < common.ss_list_.size(); ++i) delete common.ss_list_[i];

  for (std::map<sp_string, heron::proto::system::Instance*>::iterator itr =
           common.instanceid_instance_.begin();
       itr != common.instanceid_instance_.end(); ++itr)
    delete itr->second;

  // Clean up the local filesystem state
  FileUtils::removeRecursive(common.dpath_, true);
}

void ControlTopologyDone(HTTPClient* client, IncomingHTTPResponse* response) {
  if (response->response_code() != 200) {
    FAIL() << "Error while controlling topology " << response->response_code();
  } else {
    client->getEventLoop()->loopExit();
  }
  delete response;
}

void ControlTopology(sp_string topology_id, sp_int32 port, bool activate) {
  EventLoopImpl ss;
  AsyncDNS dns(&ss);
  HTTPClient* client = new HTTPClient(&ss, &dns);
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("topologyid", topology_id));
  sp_string requesturl = activate ? "/activate" : "/deactivate";
  OutgoingHTTPRequest* request =
      new OutgoingHTTPRequest(LOCALHOST, port, requesturl, BaseHTTPRequest::GET, kvs);
  auto cb = [client](IncomingHTTPResponse* response) { ControlTopologyDone(client, response); };

  if (client->SendRequest(request, std::move(cb)) != SP_OK) {
    FAIL() << "Unable to send the request\n";
  }
  ss.loop();
}

// Test to make sure that the tmaster forms the right pplan
// and sends it to all stmgrs
TEST(StMgr, test_pplan_distribute) {
  CommonResources common;
  SetUpCommonResources(common);
  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the tmaster etc.
  StartTMaster(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Wait till we get the physical plan populated on atleast one of the stmgrs
  // We just pick the first one
  while (!common.stmgrs_list_[0]->GetPhysicalPlan()) sleep(1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  // Verify that the pplan was made properly
  const heron::proto::system::PhysicalPlan* pplan0 = common.stmgrs_list_[0]->GetPhysicalPlan();
  EXPECT_EQ(pplan0->stmgrs_size(), common.num_stmgrs_);
  EXPECT_EQ(pplan0->instances_size(), common.num_stmgrs_ * num_workers_per_stmgr_);
  std::map<sp_string, heron::config::PhysicalPlanHelper::TaskData> tasks;
  heron::config::PhysicalPlanHelper::GetLocalTasks(*pplan0, common.stmgrs_id_list_[0], tasks);
  EXPECT_EQ((int)tasks.size(), (common.num_spouts_ * common.num_spout_instances_ +
                                common.num_bolts_ * common.num_bolt_instances_) /
                                   common.num_stmgrs_);

  // Delete the common resources
  TearCommonResources(common);
}

// Test to see if activate/deactivate works
// and that its distributed to tmasters
TEST(StMgr, test_activate_deactivate) {
  CommonResources common;
  SetUpCommonResources(common);

  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the tmaster etc.
  StartTMaster(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Wait till we get the physical plan populated on the stmgrs
  // and lets check that the topology is in running state
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    while (!common.stmgrs_list_[i]->GetPhysicalPlan()) sleep(1);
    EXPECT_EQ(common.stmgrs_list_[i]->GetPhysicalPlan()->topology().state(),
              heron::proto::api::RUNNING);
  }

  std::thread* deactivate_thread =
      new std::thread(ControlTopology, common.topology_id_, common.tmaster_controller_port_, false);
  // deactivate_thread->start();
  deactivate_thread->join();
  delete deactivate_thread;

  // Wait for some time and check that the topology is in deactivated state
  sleep(1);
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    EXPECT_EQ(common.stmgrs_list_[i]->GetPhysicalPlan()->topology().state(),
              heron::proto::api::PAUSED);
  }

  std::thread* activate_thread =
      new std::thread(ControlTopology, common.topology_id_, common.tmaster_controller_port_, true);
  // activate_thread->start();
  activate_thread->join();
  delete activate_thread;

  // Wait for some time and check that the topology is in deactivated state
  sleep(1);
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    EXPECT_EQ(common.stmgrs_list_[i]->GetPhysicalPlan()->topology().state(),
              heron::proto::api::RUNNING);
  }

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  // Verify that the pplan was made properly
  const heron::proto::system::PhysicalPlan* pplan0 = common.stmgrs_list_[0]->GetPhysicalPlan();
  EXPECT_EQ(pplan0->stmgrs_size(), common.num_stmgrs_);
  EXPECT_EQ(pplan0->instances_size(), common.num_stmgrs_ * num_workers_per_stmgr_);
  std::map<sp_string, heron::config::PhysicalPlanHelper::TaskData> tasks;
  heron::config::PhysicalPlanHelper::GetLocalTasks(*pplan0, common.stmgrs_id_list_[0], tasks);
  EXPECT_EQ((int)tasks.size(), (common.num_spouts_ * common.num_spout_instances_ +
                                common.num_bolts_ * common.num_bolt_instances_) /
                                   common.num_stmgrs_);

  // Delete the common resources
  TearCommonResources(common);
}

// TODO(vikasr): Add unit tests for metrics manager connection

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }

  // Create the sington for heron_internals_config_reader, if it does not exist
  if (!heron::config::HeronInternalsConfigReader::Exists()) {
    heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename);
  }
  return RUN_ALL_TESTS();
}
