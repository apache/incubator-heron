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
#include "manager/tmanager.h"
#include "manager/stmgr.h"

const sp_string SPOUT_NAME = "spout";
const sp_string BOLT_NAME = "bolt";
const sp_string STREAM_NAME = "stream";
const sp_string CONTAINER_INDEX = "0";
const sp_string STMGR_NAME = "stmgr";
const sp_string MESSAGE_TIMEOUT = "30";  // seconds
const sp_string LOCALHOST = "127.0.0.1";
const sp_string heron_internals_config_filename =
    "../../../../../../../../heron/config/heron_internals.yaml";
const sp_string metrics_sinks_config_filename =
    "../../../../../../../../heron/config/metrics_sinks.yaml";

const sp_string topology_init_config_1 = "topology.runtime.test_config";
const sp_string topology_init_config_2 = "topology.runtime.test_config2";
const sp_string spout_init_config = "topology.runtime.spout.test_config";
const sp_string bolt_init_config = "topology.runtime.bolt.test_config";

// Generate a dummy topology
static heron::proto::api::Topology* GenerateDummyTopology(
    const std::string& topology_name, const std::string& topology_id, int num_spouts,
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
    std::string compname = SPOUT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::OutputStream* ostream = spout->add_outputs();
    heron::proto::api::StreamId* tstream = ostream->mutable_stream();
    std::string streamid = STREAM_NAME;
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
    // Add runtime config
    heron::proto::api::Config::KeyValue* kv1 = config->add_kvs();
    kv1->set_key(spout_init_config);
    kv1->set_value("-1");
  }
  // Set bolts
  for (size_t i = 0; i < bolts_size; ++i) {
    heron::proto::api::Bolt* bolt = topology->add_bolts();
    // Set the component information
    heron::proto::api::Component* component = bolt->mutable_comp();
    std::string compname = BOLT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::InputStream* istream = bolt->add_inputs();
    heron::proto::api::StreamId* tstream = istream->mutable_stream();
    std::string streamid = STREAM_NAME;
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    std::string input_compname = SPOUT_NAME;
    input_compname += std::to_string(i);
    tstream->set_component_name(input_compname);
    istream->set_gtype(grouping);
    // Set the config
    heron::proto::api::Config* config = component->mutable_config();
    heron::proto::api::Config::KeyValue* kv = config->add_kvs();
    kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
    kv->set_value(std::to_string(num_bolt_instances));
    // Add runtime config
    heron::proto::api::Config::KeyValue* kv1 = config->add_kvs();
    kv1->set_key(bolt_init_config);
    kv1->set_value("-1");
  }
  // Set message timeout
  heron::proto::api::Config* topology_config = topology->mutable_topology_config();
  heron::proto::api::Config::KeyValue* kv = topology_config->add_kvs();
  kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS);
  kv->set_value(MESSAGE_TIMEOUT);
  // Add runtime config
  heron::proto::api::Config::KeyValue* kv1 = topology_config->add_kvs();
  kv1->set_key(topology_init_config_1);
  kv1->set_value("-1");
  heron::proto::api::Config::KeyValue* kv2 = topology_config->add_kvs();
  kv2->set_key(topology_init_config_2);
  kv2->set_value("-1");

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

static heron::proto::system::PackingPlan* GenerateDummyPackingPlan(int num_stmgrs_, int num_spouts,
    int num_spout_instances, int num_bolts, int num_bolt_instances) {
  size_t spouts_size = num_spouts * num_spout_instances;
  size_t bolts_size = num_bolts * num_bolt_instances;

  heron::proto::system::Resource* instanceResource = new heron::proto::system::Resource();
  instanceResource->set_ram(1);
  instanceResource->set_cpu(1);
  instanceResource->set_disk(1);

  heron::proto::system::Resource* containerRequiredResource = new heron::proto::system::Resource();
  containerRequiredResource->set_ram(10);
  containerRequiredResource->set_cpu(10);
  containerRequiredResource->set_disk(10);

  sp_int32 task_id = 0;
  sp_int32 component_index = 0;
  sp_int32 container_index = 0;

  heron::proto::system::PackingPlan* packingPlan = new heron::proto::system::PackingPlan();
  packingPlan->set_id("dummy_packing_plan_id");

  std::map<size_t, heron::proto::system::ContainerPlan*> container_map_;
  for (size_t i = 0; i < num_stmgrs_; ++i) {
    heron::proto::system::ContainerPlan* containerPlan = packingPlan->add_container_plans();
    containerPlan->set_id(i);
    heron::proto::system::Resource* requiredResource = containerPlan->mutable_requiredresource();
    requiredResource->set_cpu(containerRequiredResource->cpu());
    requiredResource->set_ram(containerRequiredResource->ram());
    requiredResource->set_disk(containerRequiredResource->disk());
    container_map_[i] = containerPlan;
  }

  // Set spouts
  for (size_t i = 0; i < spouts_size; ++i) {
    heron::proto::system::ContainerPlan* containerPlan = container_map_[container_index];
    heron::proto::system::InstancePlan* instancePlan = containerPlan->add_instance_plans();
    instancePlan->set_component_name("spout_x");
    instancePlan->set_task_id(task_id++);
    instancePlan->set_component_index(component_index++);
    heron::proto::system::Resource* resource = instancePlan->mutable_resource();
    resource->set_cpu(instanceResource->cpu());
    resource->set_ram(instanceResource->ram());
    resource->set_disk(instanceResource->disk());
    if (++container_index == num_stmgrs_) {
      container_index = 0;
    }
  }

  // Set bolts
  component_index = 0;
  for (size_t i = 0; i < bolts_size; ++i) {
    heron::proto::system::ContainerPlan* containerPlan = container_map_[container_index];
    heron::proto::system::InstancePlan* instancePlan = containerPlan->add_instance_plans();
    instancePlan->set_component_name("bolt_x");
    instancePlan->set_task_id(task_id++);
    instancePlan->set_component_index(component_index++);
    heron::proto::system::Resource* resource = instancePlan->mutable_resource();
    resource->set_cpu(instanceResource->cpu());
    resource->set_ram(instanceResource->ram());
    resource->set_disk(instanceResource->disk());
    if (++container_index == num_stmgrs_) {
      container_index = 0;
    }
  }

  return packingPlan;
}

// Method to create the local zk state on the filesystem
const std::string CreateLocalStateOnFS(heron::proto::api::Topology* topology,
                                       heron::proto::system::PackingPlan* packingPlan) {
  auto ss = std::make_shared<EventLoopImpl>();

  // Create a temporary directory to write out the state
  char dpath[255];
  snprintf(dpath, sizeof(dpath), "%s", "/tmp/XXXXXX");
  mkdtemp(dpath);

  // Write the dummy topology/tmanager location out to the local file system
  // via thestate mgr
  heron::common::HeronLocalFileStateMgr state_mgr(dpath, ss);
  state_mgr.CreateTopology(*topology, NULL);
  state_mgr.CreatePackingPlan(topology->name(), *packingPlan, NULL);

  // Return the root path
  return std::string(dpath);
}

const std::string CreateInstanceId(sp_int8 type, sp_int8 instance, bool spout) {
  std::ostringstream instanceid_stream;
  if (spout) {
    instanceid_stream << CONTAINER_INDEX << "_" << SPOUT_NAME << static_cast<int>(type);
  } else {
    instanceid_stream << CONTAINER_INDEX << "_" << BOLT_NAME << static_cast<int>(type);
  }
  instanceid_stream << "_" << static_cast<int>(instance);
  return instanceid_stream.str();
}

heron::proto::system::Instance* CreateInstanceMap(sp_int8 type, sp_int8 instance,
    sp_int32 stmgr_id, sp_int32 global_index, bool spout) {
  heron::proto::system::Instance* imap = new heron::proto::system::Instance();
  imap->set_instance_id(CreateInstanceId(type, instance, spout));

  imap->set_stmgr_id(STMGR_NAME + "-" + std::to_string(stmgr_id));
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
void StartServer(std::shared_ptr<EventLoopImpl> ss) {
  // In the ss register a dummy timer. This is to make sure that the
  // exit from the loop happens timely. If there are no timers registered
  // with the select server and also no activity on any of the fds registered
  // for read/write then after the  loopExit the loop continues indefinetly.
  ss->registerTimer([](EventLoop::Status status) { DummyTimerCb(status); }, true, 1 * 1000 * 1000);
  ss->loop();
}

void StartTManager(std::shared_ptr<EventLoopImpl>& ss, heron::tmanager::TManager*& tmanager,
                  std::thread*& tmanager_thread, const std::string& zkhostportlist,
                  const std::string& topology_name, const std::string& topology_id,
                  const std::string& dpath, const std::string& tmanager_host, sp_int32 tmanager_port,
                  sp_int32 tmanager_controller_port, sp_int32 ckptmgr_port) {
  ss = std::make_shared<EventLoopImpl>();
  tmanager = new heron::tmanager::TManager(zkhostportlist, topology_name, topology_id, dpath,
                                  tmanager_controller_port, tmanager_port, tmanager_port + 2,
                                  tmanager_port + 3, ckptmgr_port,
                                  metrics_sinks_config_filename, LOCALHOST, ss);
  tmanager_thread = new std::thread(StartServer, ss);
  // tmanager_thread->start();
}

void StartDummyStMgr(std::shared_ptr<EventLoopImpl>& ss, heron::testing::DummyStMgr*& mgr,
                     std::thread*& stmgr_thread, const std::string tmanager_host,
                     sp_int32 tmanager_port, const std::string& stmgr_id, sp_int32 stmgr_port,
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
  std::string tmanager_host_;
  sp_int32 tmanager_port_;
  sp_int32 tmanager_controller_port_;
  sp_int32 ckptmgr_port_;
  sp_int32 stmgr_baseport_;
  std::string zkhostportlist_;
  std::string topology_name_;
  std::string topology_id_;
  sp_int32 num_stmgrs_;
  sp_int32 num_spouts_;
  sp_int32 num_spout_instances_;
  sp_int32 num_bolts_;
  sp_int32 num_bolt_instances_;

  heron::proto::api::Grouping grouping_;

  // returns - filled in by init
  std::string dpath_;
  std::vector<std::shared_ptr<EventLoopImpl>> ss_list_;
  std::vector<std::string> stmgrs_id_list_;
  heron::proto::api::Topology* topology_;
  heron::proto::system::PackingPlan* packing_plan_;

  heron::tmanager::TManager* tmanager_;
  std::thread* tmanager_thread_;

  // Stmgr
  std::vector<heron::testing::DummyStMgr*> stmgrs_list_;
  std::vector<std::thread*> stmgrs_threads_list_;

  // Stmgr to instance ids
  std::map<sp_int32, std::vector<std::string> > stmgr_instance_id_list_;

  // Stmgr to Instance
  std::map<sp_int32, std::vector<heron::proto::system::Instance*> > stmgr_instance_list_;

  // Instanceid to instance
  std::map<std::string, heron::proto::system::Instance*> instanceid_instance_;

  std::map<std::string, sp_int32> instanceid_stmgr_;

  CommonResources() : topology_(NULL), tmanager_(NULL), tmanager_thread_(NULL) {
    // Create the sington for heron_internals_config_reader
    // if it does not exist
    if (!heron::config::HeronInternalsConfigReader::Exists()) {
      heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename, "");
    }
  }
};

void StartTManager(CommonResources& common) {
  // Generate a dummy topology
  common.topology_ = GenerateDummyTopology(
      common.topology_name_, common.topology_id_, common.num_spouts_, common.num_spout_instances_,
      common.num_bolts_, common.num_bolt_instances_, common.grouping_);
  common.packing_plan_ = GenerateDummyPackingPlan(common.num_stmgrs_, common.num_spouts_,
      common.num_spout_instances_, common.num_bolts_, common.num_bolt_instances_);

  // Create the zk state on the local file system
  common.dpath_ = CreateLocalStateOnFS(common.topology_, common.packing_plan_);

  // Populate the list of stmgrs
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    std::string id = STMGR_NAME + "-";
    id += std::to_string(i);
    common.stmgrs_id_list_.push_back(id);
  }

  // Start the tmanager
  std::shared_ptr<EventLoopImpl> tmanager_eventLoop;

  StartTManager(tmanager_eventLoop, common.tmanager_, common.tmanager_thread_, common.zkhostportlist_,
               common.topology_name_, common.topology_id_, common.dpath_,
               common.tmanager_host_, common.tmanager_port_, common.tmanager_controller_port_,
               common.ckptmgr_port_);
  common.ss_list_.push_back(tmanager_eventLoop);
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
    std::shared_ptr<EventLoopImpl> stmgr_ss;
    heron::testing::DummyStMgr* mgr = NULL;
    std::thread* stmgr_thread = NULL;
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
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 53001;
  common.tmanager_controller_port_ = 53002;
  common.ckptmgr_port_ = 53003;
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
  delete common.packing_plan_;
  delete common.tmanager_thread_;
  delete common.tmanager_;

  // Cleanup the stream managers
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    delete common.stmgrs_list_[i];
    delete common.stmgrs_threads_list_[i];
  }

  common.ss_list_.clear();

  for (std::map<std::string, heron::proto::system::Instance*>::iterator itr =
           common.instanceid_instance_.begin();
       itr != common.instanceid_instance_.end(); ++itr)
    delete itr->second;

  // Clean up the local filesystem state
  FileUtils::removeRecursive(common.dpath_, true);
}

void ControlTopologyDone(HTTPClient* client, IncomingHTTPResponse* response) {
  if (response->response_code() != 200) {
    FAIL() << "Error while controlling topology " << response->response_code()
           << " " << response->body();
  } else {
    client->getEventLoop()->loopExit();
  }

  delete response;
}

void ControlTopology(std::string topology_id, sp_int32 port, bool activate) {
  auto ss = std::make_shared<EventLoopImpl>();
  AsyncDNS dns(ss);
  HTTPClient* client = new HTTPClient(ss, &dns);
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("topologyid", topology_id));
  std::string requesturl = activate ? "/activate" : "/deactivate";
  OutgoingHTTPRequest* request =
      new OutgoingHTTPRequest(LOCALHOST, port, requesturl, BaseHTTPRequest::GET, kvs);
  auto cb = [client](IncomingHTTPResponse* response) { ControlTopologyDone(client, response); };

  if (client->SendRequest(request, std::move(cb)) != SP_OK) {
    FAIL() << "Unable to send the request\n";
  }
  ss->loop();
}

void UpdateRuntimeConfigDone(HTTPClient* client,
                             IncomingHTTPResponse* response,
                             sp_int32 code,
                             const std::string& message) {
  client->getEventLoop()->loopExit();
  if (response->response_code() != code) {
    FAIL() << "Got unexected result while runtime config topology for " << message
           << ". Expected: " << code << ". Got " << response->response_code()
           << ". " << response->body();
  }

  delete response;
}

void UpdateRuntimeConfig(std::string topology_id,
                         sp_int32 port,
                         std::vector<std::string> configs,
                         sp_int32 code,
                         const std::string& message) {
  auto ss = std::make_shared<EventLoopImpl>();
  AsyncDNS dns(ss);
  HTTPClient* client = new HTTPClient(ss, &dns);
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("topologyid", topology_id));
  for (std::vector<std::string>::iterator iter = configs.begin(); iter != configs.end(); ++iter) {
    kvs.push_back(make_pair("runtime-config", *iter));
  }

  std::string requesturl = "/runtime_config/update";
  OutgoingHTTPRequest* request =
      new OutgoingHTTPRequest(LOCALHOST, port, requesturl, BaseHTTPRequest::GET, kvs);

  auto cb = [client, code, message](IncomingHTTPResponse* response) {
    UpdateRuntimeConfigDone(client, response, code, message);
  };

  if (client->SendRequest(request, std::move(cb)) != SP_OK) {
    FAIL() << "Unable to send the runtime config request\n";
  }
  ss->loop();
}


// Test to make sure that the tmanager forms the right pplan
// and sends it to all stmgrs
TEST(StMgr, test_pplan_distribute) {
  CommonResources common;
  SetUpCommonResources(common);
  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the tmanager etc.
  StartTManager(common);

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
  common.tmanager_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  // Verify that the pplan was made properly
  const heron::proto::system::PhysicalPlan* pplan0 = common.stmgrs_list_[0]->GetPhysicalPlan();
  EXPECT_EQ(pplan0->stmgrs_size(), common.num_stmgrs_);
  EXPECT_EQ(pplan0->instances_size(), common.num_stmgrs_ * num_workers_per_stmgr_);
  std::map<std::string, heron::config::PhysicalPlanHelper::TaskData> tasks;
  heron::config::PhysicalPlanHelper::GetLocalTasks(*pplan0, common.stmgrs_id_list_[0], tasks);
  EXPECT_EQ((int)tasks.size(), (common.num_spouts_ * common.num_spout_instances_ +
                                common.num_bolts_ * common.num_bolt_instances_) /
                                   common.num_stmgrs_);

  // Delete the common resources
  TearCommonResources(common);
}

// Test to see if activate/deactivate works
// and that its distributed to tmanagers
TEST(StMgr, test_activate_deactivate) {
  CommonResources common;
  SetUpCommonResources(common);

  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the tmanager etc.
  StartTManager(common);

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
      new std::thread(ControlTopology, common.topology_id_, common.tmanager_controller_port_, false);
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
      new std::thread(ControlTopology, common.topology_id_, common.tmanager_controller_port_, true);
  // activate_thread->start();
  activate_thread->join();
  delete activate_thread;

  // Wait for some time and check that the topology is in activated state
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
  common.tmanager_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  // Verify that the pplan was made properly
  const heron::proto::system::PhysicalPlan* pplan0 = common.stmgrs_list_[0]->GetPhysicalPlan();
  EXPECT_EQ(pplan0->stmgrs_size(), common.num_stmgrs_);
  EXPECT_EQ(pplan0->instances_size(), common.num_stmgrs_ * num_workers_per_stmgr_);
  std::map<std::string, heron::config::PhysicalPlanHelper::TaskData> tasks;
  heron::config::PhysicalPlanHelper::GetLocalTasks(*pplan0, common.stmgrs_id_list_[0], tasks);
  EXPECT_EQ((int)tasks.size(), (common.num_spouts_ * common.num_spout_instances_ +
                                common.num_bolts_ * common.num_bolt_instances_) /
                                   common.num_stmgrs_);

  // Delete the common resources
  TearCommonResources(common);
}

// Test to see if runtime_config works
TEST(StMgr, test_runtime_config) {
  std::string runtime_test_spout = "spout3";
  std::string runtime_test_bolt = "bolt4";

  CommonResources common;
  SetUpCommonResources(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Wait till we get the physical plan populated on the stmgrs, then
  // verify current config values in tmanager as well as stream managers.
  // common.tmanager_->FetchPhysicalPlan();
  // auto t = init_pplan->topology();
  // auto c = t.topology_config();
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    while (!common.stmgrs_list_[i]->GetPhysicalPlan()) sleep(1);
  }

  // Test ValidateRuntimeConfig()
  heron::tmanager::ComponentConfigMap validate_good_config_map;
  std::map<std::string, std::string> validate_good_config;
  validate_good_config[topology_init_config_1] = "1";
  validate_good_config[topology_init_config_2] = "2";
  const char* topology_key = heron::config::TopologyConfigHelper::GetReservedTopologyConfigKey();
  validate_good_config_map[topology_key] = validate_good_config;
  validate_good_config_map["spout1"] = validate_good_config;
  EXPECT_EQ(common.tmanager_->ValidateRuntimeConfig(validate_good_config_map), true);

  heron::tmanager::ComponentConfigMap validate_bad_config_map;
  std::map<std::string, std::string> validate_bad_config;
  validate_good_config[topology_init_config_1] = "1";
  validate_bad_config_map["unknown_component"] = validate_good_config;
  EXPECT_EQ(common.tmanager_->ValidateRuntimeConfig(validate_bad_config_map), false);

  // Post runtime config request with no configs and expect 400 response.
  std::vector<std::string> no_config;
  std::thread* no_config_update_thread = new std::thread(UpdateRuntimeConfig,
      common.topology_id_, common.tmanager_controller_port_, no_config, 400, "no_config");
  no_config_update_thread->join();
  delete no_config_update_thread;

  // Post runtime config request with unavailable configs and expect 400 response.
  std::vector<std::string> wrong_config1;
  wrong_config1.push_back("badformat");  // Bad format
  std::thread* wrong_config1_update_thread = new std::thread(UpdateRuntimeConfig,
      common.topology_id_, common.tmanager_controller_port_, wrong_config1, 400, "wrong_config1");
  wrong_config1_update_thread->join();
  delete wrong_config1_update_thread;

  std::vector<std::string> wrong_config2;
  wrong_config2.push_back("topology.runtime.test_config:1");
  // Component doesn't exist
  wrong_config2.push_back("bad_component:topology.runtime.bolt.test_config:1");
  std::thread* wrong_config2_update_thread = new std::thread(UpdateRuntimeConfig,
      common.topology_id_, common.tmanager_controller_port_, wrong_config2, 400, "wrong_config2");
  wrong_config2_update_thread->join();
  delete wrong_config2_update_thread;

  // Post runtime config request with good configs and expect 200 response.
  std::vector<std::string> good_config;
  good_config.push_back(topology_init_config_1 + ":1");
  good_config.push_back(topology_init_config_2 + ":2");
  good_config.push_back(runtime_test_spout + ":" + spout_init_config + ":3");
  good_config.push_back(runtime_test_bolt + ":" + bolt_init_config + ":4");
  std::thread* good_config_update_thread = new std::thread(UpdateRuntimeConfig,
      common.topology_id_, common.tmanager_controller_port_, good_config, 200, "good_config");
  good_config_update_thread->join();
  delete good_config_update_thread;

  // Wait till we get the physical plan populated on the stmgrs
  // and lets check that the topology has the updated configs.
  sleep(1);
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    std::map<std::string, std::string> updated_config, updated_spout_config, updated_bolt_config;
    const heron::proto::system::PhysicalPlan* pplan = common.stmgrs_list_[i]->GetPhysicalPlan();
    heron::config::TopologyConfigHelper::GetTopologyRuntimeConfig(pplan->topology(),
        updated_config);
    EXPECT_EQ(updated_config[topology_init_config_1 + ":runtime"], "1");
    EXPECT_EQ(updated_config[topology_init_config_2 + ":runtime"], "2");
    heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(pplan->topology(),
        runtime_test_spout, updated_spout_config);
    EXPECT_EQ(updated_spout_config[spout_init_config + ":runtime"], "3");
    heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(pplan->topology(),
        runtime_test_bolt, updated_bolt_config);
    EXPECT_EQ(updated_bolt_config[bolt_init_config + ":runtime"], "4");
  }
  std::map<std::string, std::string> updated_config, updated_spout_config, updated_bolt_config;
  const auto pplan = common.tmanager_->getPhysicalPlan();
  heron::config::TopologyConfigHelper::GetTopologyRuntimeConfig(pplan->topology(), updated_config);
  EXPECT_EQ(updated_config[topology_init_config_1 + ":runtime"], "1");
  EXPECT_EQ(updated_config[topology_init_config_2 + ":runtime"], "2");
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(pplan->topology(),
      runtime_test_spout, updated_spout_config);
  EXPECT_EQ(updated_spout_config[spout_init_config + ":runtime"], "3");
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(pplan->topology(),
      runtime_test_bolt, updated_bolt_config);
  EXPECT_EQ(updated_bolt_config[bolt_init_config + ":runtime"], "4");

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
}

// TODO(vikasr): Add unit tests for metrics manager connection

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  std::cout << "Current working directory (to find tmanager logs) "
      << ProcessUtils::getCurrentWorkingDirectory() << std::endl;
  testing::InitGoogleTest(&argc, argv);
  sp_string configFile = heron_internals_config_filename;
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    configFile = argv[1];
  }

  // Create the sington for heron_internals_config_reader, if it does not exist
  if (!heron::config::HeronInternalsConfigReader::Exists()) {
    heron::config::HeronInternalsConfigReader::Create(configFile, "");
  }
  return RUN_ALL_TESTS();
}
