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
#include "config/heron-internals-config-reader.h"
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "config/physical-plan-helper.h"
#include "statemgr/heron-statemgr.h"
#include "statemgr/heron-localfilestatemgr.h"
#include "manager/tmanager.h"
#include "manager/stmgr.h"
#include "server/dummy_instance.h"
#include "server/dummy_stmgr.h"
#include "server/dummy_metricsmgr.h"

const sp_string SPOUT_NAME = "spout";
const sp_string BOLT_NAME = "bolt";
const sp_string STREAM_NAME = "stream";
const sp_string CONTAINER_INDEX = "0";
const sp_string STMGR_NAME = "stmgr";
const sp_string MESSAGE_TIMEOUT = "30";  // seconds
const sp_string LOCALHOST = "127.0.0.1";
sp_string heron_internals_config_filename =
    "heron/config/src/yaml/conf/test/test_heron_internals.yaml";
sp_string metrics_sinks_config_filename =
    "heron/config/src/yaml/conf/test/test_metrics_sinks.yaml";

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
  kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS);
  kv->set_value(MESSAGE_TIMEOUT);

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

static heron::proto::system::PackingPlan* GenerateDummyPackingPlan(size_t num_stmgrs_,
    size_t num_spouts, size_t num_spout_instances, size_t num_bolts, size_t num_bolt_instances) {
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

  sp_uint32 task_id = 0;
  sp_uint32 component_index = 0;
  sp_uint32 container_index = 0;

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
void CreateLocalStateOnFS(heron::proto::api::Topology* topology,
                          heron::proto::system::PackingPlan* packingPlan, sp_string dpath) {
  auto ss = std::make_shared<EventLoopImpl>();

  // Write the dummy topology/tmanager location out to the local file system via the state mgr
  heron::common::HeronLocalFileStateMgr state_mgr(dpath, ss);
  state_mgr.CreateTopology(*topology, NULL);
  state_mgr.CreatePackingPlan(topology->name(), *packingPlan, NULL);
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

std::shared_ptr<heron::proto::system::Instance> CreateInstanceMap(sp_int8 type, sp_int8 instance,
                                             sp_int32 stmgr_id, sp_int32 global_index, bool spout) {
  auto imap = std::make_shared<heron::proto::system::Instance>();
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
                  std::thread*& tmanager_thread, const sp_string& zkhostportlist,
                  const sp_string& topology_name, const sp_string& topology_id,
                  const sp_string& dpath,
                  sp_int32 tmanager_port, sp_int32 tmanager_controller_port,
                  sp_int32 tmanager_stats_port, sp_int32 metrics_mgr_port,
                  sp_int32 ckptmgr_port) {
  ss = std::make_shared<EventLoopImpl>();
  tmanager = new heron::tmanager::TManager(zkhostportlist, topology_name, topology_id, dpath,
                                  tmanager_controller_port, tmanager_port, tmanager_stats_port,
                                  metrics_mgr_port, ckptmgr_port,
                                  metrics_sinks_config_filename, LOCALHOST, ss);
  tmanager_thread = new std::thread(StartServer, ss);
}

void StartStMgr(std::shared_ptr<EventLoopImpl>& ss, heron::stmgr::StMgr*& mgr,
                std::thread*& stmgr_thread,
                const sp_string& stmgr_host, sp_int32& stmgr_port, sp_int32& local_data_port,
                const sp_string& topology_name,
                const sp_string& topology_id, const heron::proto::api::Topology* topology,
                const std::vector<sp_string>& workers, const sp_string& stmgr_id,
                const sp_string& zkhostportlist, const sp_string& dpath, sp_int32 metricsmgr_port,
                sp_int32 shell_port, sp_int32 ckptmgr_port, const sp_string& ckptmgr_id,
                sp_int64 _high_watermark, sp_int64 _low_watermark) {
  // The topology will be owned and deleted by the strmgr
  auto stmgr_topology = std::make_shared<heron::proto::api::Topology>();
  stmgr_topology->CopyFrom(*topology);
  // Create the select server for this stmgr to use
  ss = std::make_shared<EventLoopImpl>();
  mgr = new heron::stmgr::StMgr(ss, stmgr_host, stmgr_port, local_data_port, topology_name,
                                topology_id,
                                stmgr_topology, stmgr_id, workers, zkhostportlist, dpath,
                                metricsmgr_port, shell_port, ckptmgr_port, ckptmgr_id,
                                _high_watermark, _low_watermark, "disabled");
  EXPECT_EQ(0, stmgr_port);
  EXPECT_EQ(0, local_data_port);
  mgr->Init();
  stmgr_port = mgr->GetStmgrServerNetworkOptions().get_port();
  local_data_port = mgr->GetInstanceServerNetworkOptions().get_port();
  EXPECT_GT(stmgr_port, 0);
  stmgr_thread = new std::thread(StartServer, ss);
}

void StartDummyStMgr(std::shared_ptr<EventLoopImpl>& ss, DummyStMgr*& mgr,
                     std::thread*& stmgr_thread,
                     sp_int32& stmgr_port, sp_int32 tmanager_port, sp_int32 shell_port,
                     const sp_string& stmgr_id,
                     const std::vector<shared_ptr<heron::proto::system::Instance>>& instances) {
  // Create the select server for this stmgr to use
  ss = std::make_shared<EventLoopImpl>();

  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(1_MB);
  options.set_socket_family(PF_INET);

  mgr = new DummyStMgr(ss, options, stmgr_id, LOCALHOST, stmgr_port, LOCALHOST, tmanager_port,
                       shell_port, instances);
  EXPECT_EQ(0, stmgr_port);
  EXPECT_EQ(0, mgr->Start()) << "DummyStMgr bind " << LOCALHOST << ":" << stmgr_port;
  stmgr_port = mgr->get_serveroptions().get_port();
  EXPECT_GT(stmgr_port, 0);
  stmgr_thread = new std::thread(StartServer, ss);
}

void StartDummyMtrMgr(std::shared_ptr<EventLoopImpl>& ss, DummyMtrMgr*& mgr,
                      std::thread*& mtmgr_thread,
                      sp_int32& mtmgr_port, const sp_string& stmgr_id, CountDownLatch* tmanagerLatch,
                      CountDownLatch* connectionCloseLatch) {
  // Create the select server for this stmgr to use
  ss = std::make_shared<EventLoopImpl>();

  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(mtmgr_port);
  options.set_max_packet_size(10_MB);
  options.set_socket_family(PF_INET);

  mgr = new DummyMtrMgr(ss, options, stmgr_id, tmanagerLatch, connectionCloseLatch);
  EXPECT_EQ(0, mgr->Start()) << "DummyMtrMgr bind " << LOCALHOST << ":" << mtmgr_port;
  mtmgr_port = mgr->get_serveroptions().get_port();
  EXPECT_GT(mtmgr_port, 0);
  mtmgr_thread = new std::thread(StartServer, ss);
}

void StartDummySpoutInstance(std::shared_ptr<EventLoopImpl>& ss, DummySpoutInstance*& worker,
                             std::thread*& worker_thread, sp_int32 stmgr_port,
                             const sp_string& topology_name, const sp_string& topology_id,
                             const sp_string& instance_id, const sp_string& component_name,
                             sp_int32 task_id, sp_int32 component_index, const sp_string& stmgr_id,
                             const sp_string& stream_id, sp_int32 max_msgs_to_send,
                             bool _do_custom_grouping) {
  // Create the select server for this worker to use
  ss = std::make_shared<EventLoopImpl>();
  // Create the network option
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(10_MB);
  options.set_socket_family(PF_INET);

  worker = new DummySpoutInstance(ss, options, topology_name, topology_id, instance_id,
                                  component_name, task_id, component_index, stmgr_id, stream_id,
                                  max_msgs_to_send, _do_custom_grouping);
  worker->Start();
  worker_thread = new std::thread(StartServer, ss);
}

void StartDummyBoltInstance(std::shared_ptr<EventLoopImpl>& ss, DummyBoltInstance*& worker,
                            std::thread*& worker_thread, sp_int32 stmgr_port,
                            const sp_string& topology_name, const sp_string& topology_id,
                            const sp_string& instance_id, const sp_string& component_name,
                            sp_int32 task_id, sp_int32 component_index, const sp_string& stmgr_id,
                            sp_int32 expected_msgs_to_recv) {
  // Create the select server for this worker to use
  ss = std::make_shared<EventLoopImpl>();
  // Create the network option
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(10_MB);
  options.set_socket_family(PF_INET);

  worker =
      new DummyBoltInstance(ss, options, topology_name, topology_id, instance_id, component_name,
                            task_id, component_index, stmgr_id, expected_msgs_to_recv);
  worker->Start();
  worker_thread = new std::thread(StartServer, ss);
}

struct CommonResources {
  // arguments
  sp_string tmanager_host_;
  sp_int32 tmanager_port_;
  sp_int32 tmanager_controller_port_;
  sp_int32 tmanager_stats_port_;
  sp_int32 metricsmgr_port_;
  sp_int32 shell_port_;
  sp_int32 ckptmgr_port_;
  sp_string ckptmgr_id_;
  sp_string zkhostportlist_;
  sp_string topology_name_;
  sp_string topology_id_;
  size_t num_stmgrs_;
  size_t num_spouts_;
  size_t num_spout_instances_;
  size_t num_bolts_;
  size_t num_bolt_instances_;

  // store the stmgr server port returned by bind/listen 0
  std::vector<sp_int32> stmgr_ports_;
  std::vector<sp_int32> local_data_ports_;

  heron::proto::api::Grouping grouping_;

  // returns - filled in by init
  sp_string dpath_;
  std::vector<std::shared_ptr<EventLoopImpl>> ss_list_;
  std::vector<sp_string> stmgrs_id_list_;
  heron::proto::api::Topology* topology_;
  heron::proto::system::PackingPlan* packing_plan_;

  heron::tmanager::TManager* tmanager_;
  std::thread* tmanager_thread_;

  // Component
  std::vector<DummySpoutInstance*> spout_workers_list_;
  std::vector<std::thread*> spout_workers_threads_list_;
  std::vector<DummyBoltInstance*> bolt_workers_list_;
  std::vector<std::thread*> bolt_workers_threads_list_;

  // metrics mgr
  DummyMtrMgr* metrics_mgr_;
  std::thread* metrics_mgr_thread_;

  // Stmgr
  std::vector<heron::stmgr::StMgr*> stmgrs_list_;
  std::vector<std::thread*> stmgrs_threads_list_;

  // Stmgr to instance ids
  std::map<sp_int32, std::vector<sp_string> > stmgr_instance_id_list_;

  // Stmgr to Instance
  std::map<sp_int32, std::vector<shared_ptr<heron::proto::system::Instance>>> stmgr_instance_list_;

  // Instanceid to instance
  std::map<sp_string, shared_ptr<heron::proto::system::Instance>> instanceid_instance_;

  std::map<sp_string, sp_int32> instanceid_stmgr_;

  sp_int64 high_watermark_;
  sp_int64 low_watermark_;

  CommonResources() : topology_(NULL), tmanager_(NULL), tmanager_thread_(NULL) {
    // Create the sington for heron_internals_config_reader
    // if it does not exist
    if (!heron::config::HeronInternalsConfigReader::Exists()) {
      heron::config::HeronInternalsConfigReader::Create(heron_internals_config_filename, "");
    }

    // Create a temporary directory to write out the state
    char dpath[255];
    snprintf(dpath, sizeof(dpath), "%s", "/tmp/XXXXXX");
    mkdtemp(dpath);
    dpath_ = sp_string(dpath);
    // Lets change the Connection buffer HWM and LWN for back pressure to get the
    // test case done faster
    high_watermark_ = 10_MB;
    low_watermark_ = 5_MB;
  }

  void setNumStmgrs(sp_int32 numStmgrs) {
    num_stmgrs_ = numStmgrs;
    while (numStmgrs > 0) {
      stmgr_ports_.push_back(0);
      local_data_ports_.push_back(0);
      numStmgrs--;
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
  CreateLocalStateOnFS(common.topology_, common.packing_plan_, common.dpath_);

  // Populate the list of stmgrs
  for (size_t i = 0; i < common.num_stmgrs_; ++i) {
    sp_string id = STMGR_NAME + "-";
    id += std::to_string(i);
    common.stmgrs_id_list_.push_back(id);
  }

  // Start the tmanager
  std::shared_ptr<EventLoopImpl> tmanager_eventLoop;

  StartTManager(tmanager_eventLoop, common.tmanager_, common.tmanager_thread_, common.zkhostportlist_,
               common.topology_name_, common.topology_id_, common.dpath_,
               common.tmanager_port_, common.tmanager_controller_port_, common.tmanager_stats_port_,
               common.metricsmgr_port_, common.ckptmgr_port_);
  common.ss_list_.push_back(tmanager_eventLoop);
}

void DistributeWorkersAcrossStmgrs(CommonResources& common) {
  // which stmgr is this component going to get assigned to
  sp_uint32 stmgr_assignment_round = 0;
  sp_uint32 global_index = 0;
  // Distribute the spouts
  for (size_t spout = 0; spout < common.num_spouts_; ++spout) {
    for (size_t spout_instance = 0; spout_instance < common.num_spout_instances_;
        ++spout_instance) {
      auto imap =
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
  for (size_t bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (size_t bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      auto imap =
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

void StartDummySpoutInstanceHelper(CommonResources& common, sp_int8 spout, sp_int8 spout_instance,
                                   sp_int32 num_msgs_sent_by_spout_instance) {
  // Start the spout
  std::shared_ptr<EventLoopImpl> worker_ss;
  DummySpoutInstance* worker = NULL;
  std::thread* worker_thread = NULL;
  sp_string streamid = STREAM_NAME;
  streamid += std::to_string(spout);
  sp_string instanceid = CreateInstanceId(spout, spout_instance, true);
  StartDummySpoutInstance(worker_ss, worker, worker_thread,
                          common.local_data_ports_[common.instanceid_stmgr_[instanceid]],
                          common.topology_name_, common.topology_id_, instanceid,
                          common.instanceid_instance_[instanceid]->info().component_name(),
                          common.instanceid_instance_[instanceid]->info().task_id(),
                          common.instanceid_instance_[instanceid]->info().component_index(),
                          common.instanceid_instance_[instanceid]->stmgr_id(), streamid,
                          num_msgs_sent_by_spout_instance,
                          common.grouping_ == heron::proto::api::CUSTOM);
  common.ss_list_.push_back(worker_ss);
  common.spout_workers_list_.push_back(worker);
  common.spout_workers_threads_list_.push_back(worker_thread);
}

void StartWorkerComponents(CommonResources& common, sp_int32 num_msgs_sent_by_spout_instance,
                           sp_int32 num_msgs_to_expect_in_bolt) {
  // try to find the lowest bolt task id
  sp_int32 min_bolt_task_id = std::numeric_limits<sp_int32>::max() - 1;
  for (size_t bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (size_t bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      sp_string instanceid = CreateInstanceId(bolt, bolt_instance, false);
      if (common.instanceid_instance_[instanceid]->info().task_id() < min_bolt_task_id) {
        min_bolt_task_id = common.instanceid_instance_[instanceid]->info().task_id();
      }
    }
  }

  // Start the spouts
  for (size_t spout = 0; spout < common.num_spouts_; ++spout) {
    for (size_t spout_instance = 0; spout_instance < common.num_spout_instances_;
        ++spout_instance) {
      StartDummySpoutInstanceHelper(common, spout, spout_instance, num_msgs_sent_by_spout_instance);
    }
  }
  // Start the bolts
  std::vector<DummyBoltInstance*> bolt_workers_list;
  std::vector<std::thread*> bolt_workers_threads_list;
  for (size_t bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (size_t bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      std::shared_ptr<EventLoopImpl> worker_ss;
      DummyBoltInstance* worker = NULL;
      std::thread* worker_thread = NULL;
      sp_string instanceid = CreateInstanceId(bolt, bolt_instance, false);
      sp_int32 nmessages_to_expect = num_msgs_to_expect_in_bolt;
      if (common.grouping_ == heron::proto::api::CUSTOM) {
        if (common.instanceid_instance_[instanceid]->info().task_id() == min_bolt_task_id) {
          nmessages_to_expect =
              num_msgs_sent_by_spout_instance * common.num_spouts_ * common.num_spout_instances_;
        } else {
          nmessages_to_expect = 0;
        }
      }
      StartDummyBoltInstance(worker_ss, worker, worker_thread,
                             common.local_data_ports_[common.instanceid_stmgr_[instanceid]],
                             common.topology_name_, common.topology_id_, instanceid,
                             common.instanceid_instance_[instanceid]->info().component_name(),
                             common.instanceid_instance_[instanceid]->info().task_id(),
                             common.instanceid_instance_[instanceid]->info().component_index(),
                             common.instanceid_instance_[instanceid]->stmgr_id(),
                             nmessages_to_expect);
      common.ss_list_.push_back(worker_ss);
      common.bolt_workers_list_.push_back(worker);
      common.bolt_workers_threads_list_.push_back(worker_thread);
    }
  }
}

void StartStMgrs(CommonResources& common) {
  // Spawn and start the stmgrs
  for (size_t i = 0; i < common.num_stmgrs_; ++i) {
    std::shared_ptr<EventLoopImpl> stmgr_ss;
    heron::stmgr::StMgr* mgr = NULL;
    std::thread* stmgr_thread = NULL;

    StartStMgr(stmgr_ss, mgr, stmgr_thread, common.tmanager_host_, common.stmgr_ports_[i],
               common.local_data_ports_[i],
               common.topology_name_, common.topology_id_, common.topology_,
               common.stmgr_instance_id_list_[i], common.stmgrs_id_list_[i], common.zkhostportlist_,
               common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
               common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);

    common.ss_list_.push_back(stmgr_ss);
    common.stmgrs_list_.push_back(mgr);
    common.stmgrs_threads_list_.push_back(stmgr_thread);
  }
}

void StartMetricsMgr(CommonResources& common, CountDownLatch* tmanagerLatch,
                     CountDownLatch* connectionCloseLatch) {
  std::shared_ptr<EventLoopImpl> ss;
  DummyMtrMgr* mgr = NULL;
  std::thread* metrics_mgr = NULL;
  StartDummyMtrMgr(ss, mgr, metrics_mgr, common.metricsmgr_port_, "stmgr", tmanagerLatch,
                   connectionCloseLatch);
  common.ss_list_.push_back(ss);
  common.metrics_mgr_ = mgr;
  common.metrics_mgr_thread_ = metrics_mgr;
}

void StartMetricsMgr(CommonResources& common) { StartMetricsMgr(common, NULL, NULL); }

void TearCommonResources(CommonResources& common) {
  delete common.topology_;
  delete common.packing_plan_;
  delete common.tmanager_thread_;
  delete common.tmanager_;
  delete common.metrics_mgr_thread_;
  delete common.metrics_mgr_;

  // Cleanup the stream managers
  for (size_t i = 0; i < common.stmgrs_list_.size(); ++i) {
    delete common.stmgrs_list_[i];
    delete common.stmgrs_threads_list_[i];
  }

  for (size_t w = 0; w < common.spout_workers_list_.size(); ++w) {
    delete common.spout_workers_list_[w];
    delete common.spout_workers_threads_list_[w];
  }

  for (size_t w = 0; w < common.bolt_workers_list_.size(); ++w) {
    delete common.bolt_workers_list_[w];
    delete common.bolt_workers_threads_list_[w];
  }

  common.ss_list_.clear();

  common.instanceid_instance_.clear();

  // Clean up the local filesystem state
  FileUtils::removeRecursive(common.dpath_, true);
}

void VerifyMetricsMgrTManager(CommonResources& common) {
  EXPECT_NE(common.metrics_mgr_->get_tmanager(), (heron::proto::tmanager::TManagerLocation*)NULL);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->topology_name(), common.topology_name_);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->topology_id(), common.topology_id_);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->host(), LOCALHOST);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->controller_port(), common.tmanager_controller_port_);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->server_port(), common.tmanager_port_);
  EXPECT_EQ(common.metrics_mgr_->get_tmanager()->stats_port(), common.tmanager_stats_port_);
}

// Test to make sure that the stmgr can decode the pplan
TEST(StMgr, test_pplan_decode) {
  CommonResources common;
  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 10000;
  common.tmanager_controller_port_ = 10001;
  common.tmanager_stats_port_ = 10002;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 40000;
  common.ckptmgr_port_ = 50000;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 5;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 5;
  common.num_bolt_instances_ = 2;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Start dummy worker to make the stmgr connect to the tmanager
  StartWorkerComponents(common, 0, 0);

  // Wait till we get the physical plan populated on atleast one of the stmgrs
  // We just pick the first one
  while (!common.stmgrs_list_[0]->GetPhysicalPlan()) sleep(1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Verify that the pplan was decoded properly
  const shared_ptr<heron::proto::system::PhysicalPlan> pplan0 =
          common.stmgrs_list_[0]->GetPhysicalPlan();
  EXPECT_EQ(pplan0->stmgrs_size(), common.num_stmgrs_);
  for (size_t i = 0; i < common.num_stmgrs_; i++) {
    EXPECT_NE(common.stmgr_ports_.end(),
              std::find(common.stmgr_ports_.begin(), common.stmgr_ports_.end(),
                        pplan0->stmgrs(i).data_port()));
    EXPECT_NE(common.local_data_ports_.end(),
              std::find(common.local_data_ports_.begin(), common.local_data_ports_.end(),
                        pplan0->stmgrs(i).local_data_port()));
  }
  EXPECT_EQ(pplan0->instances_size(), common.num_stmgrs_ * num_workers_per_stmgr_);
  std::map<sp_string, heron::config::PhysicalPlanHelper::TaskData> tasks;
  heron::config::PhysicalPlanHelper::GetLocalTasks(*pplan0, common.stmgrs_id_list_[0], tasks);
  EXPECT_EQ((int)tasks.size(), (common.num_spouts_ * common.num_spout_instances_ +
                                common.num_bolts_ * common.num_bolt_instances_) /
                                   common.num_stmgrs_);

  // Delete the common resources
  TearCommonResources(common);
}

// Test to make sure that the stmgr can route data properly
TEST(StMgr, test_tuple_route) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 15000;
  common.tmanager_controller_port_ = 15001;
  common.tmanager_stats_port_ = 15002;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 45000;
  common.ckptmgr_port_ = 55000;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 1;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 4;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  int num_msgs_sent_by_spout_instance = 8;

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Start the dummy workers
  StartWorkerComponents(
      common, num_msgs_sent_by_spout_instance,
      (num_msgs_sent_by_spout_instance * common.num_spouts_ * common.num_spout_instances_) /
          (common.num_bolts_ * common.num_bolt_instances_));

  // Wait for the bolt thread to complete receiving
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate. We have already waited for the bolt
  // threads
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }

  // Verification
  // Verify that the worker got the right stream manager id
  //  EXPECT_EQ(common.stmgrs_id_list_[0], common.spout_workers_list_[0]->RecvdStMgrId());
  // Make sure shuffle grouping worked
  for (size_t w = 0; w < common.bolt_workers_list_.size(); ++w) {
    EXPECT_EQ((sp_uint64)common.bolt_workers_list_[w]->MsgsRecvd(),
              (num_msgs_sent_by_spout_instance * common.num_spouts_ * common.num_spout_instances_) /
                  common.bolt_workers_list_.size());
  }

  TearCommonResources(common);
}

// Test to make sure that custom grouping routing works
TEST(StMgr, test_custom_grouping_route) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 15500;
  common.tmanager_controller_port_ = 15501;
  common.tmanager_stats_port_ = 15502;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 45500;
  common.ckptmgr_port_ = 55500;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 1;  // This test will only work with 1 type of spout
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 4;
  common.grouping_ = heron::proto::api::CUSTOM;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  int num_msgs_sent_by_spout_instance = 8;

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Start the dummy workers
  StartWorkerComponents(
      common, num_msgs_sent_by_spout_instance,
      (num_msgs_sent_by_spout_instance * common.num_spouts_ * common.num_spout_instances_) /
          (common.num_bolts_ * common.num_bolt_instances_));

  // Wait for the bolt thread to complete receiving
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate. We have already waited for the bolt
  // threads
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();

  for (size_t i = 0; i < common.stmgrs_threads_list_.size(); ++i) {
    common.stmgrs_threads_list_[i]->join();
  }

  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }

  // Verification
  // Make sure custom grouping worked
  sp_int32 lowest_bolt_task_id = std::numeric_limits<sp_int32>::max() - 1;
  sp_int32 non_zero_msgs_task_id = -1;
  for (size_t w = 0; w < common.bolt_workers_list_.size(); ++w) {
    if (common.bolt_workers_list_[w]->get_task_id() < lowest_bolt_task_id) {
      lowest_bolt_task_id = common.bolt_workers_list_[w]->get_task_id();
    }

    if (common.bolt_workers_list_[w]->MsgsRecvd() != 0) {
      EXPECT_EQ(
          common.bolt_workers_list_[w]->MsgsRecvd(),
          (num_msgs_sent_by_spout_instance * common.num_spouts_ * common.num_spout_instances_));
      EXPECT_EQ(non_zero_msgs_task_id, -1);
      non_zero_msgs_task_id = common.bolt_workers_list_[w]->get_task_id();
    }
  }

  EXPECT_EQ(lowest_bolt_task_id, non_zero_msgs_task_id);

  TearCommonResources(common);
}

TEST(StMgr, test_back_pressure_instance) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 17000;
  common.tmanager_controller_port_ = 17001;
  common.tmanager_stats_port_ = 17002;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 47000;
  common.ckptmgr_port_ = 57000;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 4;
  common.num_spout_instances_ = 4;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";
  // Overwrite the default values for back pressure
  common.high_watermark_ = 1_MB;
  common.low_watermark_ = 500_KB;

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;

  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Stop the bolt schedulers at this point so that they stop receiving
  // This will build up back pressure
  for (size_t i = 0; i < common.bolt_workers_list_.size(); ++i) {
    common.bolt_workers_list_[i]->getEventLoop()->loopExit();
  }

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStartBPMsgs() == 0) sleep(1);

  // Now kill the bolts - at that point the back pressure should be removed
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }
  for (size_t w = 0; w < common.bolt_workers_list_.size(); ++w) {
    delete common.bolt_workers_list_[w];
  }
  // Clear the list so that we don't double delete in TearCommonResources
  common.bolt_workers_list_.clear();

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStopBPMsgs() == 0) sleep(1);

  EXPECT_EQ(dummy_stmgr->NumStopBPMsgs(), 1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  TearCommonResources(common);
}

// Tests that spout deaths during backpressure are handled correctly
TEST(StMgr, test_spout_death_under_backpressure) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 17300;
  common.tmanager_controller_port_ = 17301;
  common.tmanager_stats_port_ = 17302;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 47300;
  common.ckptmgr_port_ = 57300;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 4;
  common.num_spout_instances_ = 4;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";
  // Overwrite the default values for back pressure
  common.high_watermark_ = 1_MB;
  common.low_watermark_ = 500_KB;

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Stop the bolt schedulers at this point so that they stop receiving
  // This will build up back pressure
  for (size_t i = 0; i < common.bolt_workers_list_.size(); ++i) {
    common.bolt_workers_list_[i]->getEventLoop()->loopExit();
  }

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStartBPMsgs() == 0) sleep(1);

  // Now that we are in back pressure, kill the current spout
  common.spout_workers_list_[0]->getEventLoop()->loopExit();
  common.spout_workers_threads_list_[0]->join();

  // Create a new spout now
  StartDummySpoutInstanceHelper(common, 0, 0, num_msgs_sent_by_spout_instance);
  sleep(1);

  // First time the SM return NOTOK, and closes the old spout connection
  EXPECT_EQ(common.spout_workers_list_.back()->GetRegisterResponseStatus(),
            heron::proto::system::NOTOK);

  // Upon receiving NOTOK, the new spout dies the first time
  common.spout_workers_list_.back()->getEventLoop()->loopExit();
  common.spout_workers_threads_list_.back()->join();

  // Bring up the new spout again
  StartDummySpoutInstanceHelper(common, 0, 0, num_msgs_sent_by_spout_instance);
  sleep(1);

  // This time we expect SM to accept the new spout connection correctly
  EXPECT_EQ(common.spout_workers_list_.back()->GetRegisterResponseStatus(),
            heron::proto::system::OK);

  // Now kill the bolts - at that point the back pressure should be removed
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }
  for (size_t w = 0; w < common.bolt_workers_list_.size(); ++w) {
    delete common.bolt_workers_list_[w];
  }

  // Clear the list so that we don't double delete in TearCommonResources
  common.bolt_workers_list_.clear();

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStopBPMsgs() == 0) sleep(1);

  EXPECT_EQ(dummy_stmgr->NumStopBPMsgs(), 1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();

  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    if (common.spout_workers_threads_list_[i]->joinable())
      common.spout_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  TearCommonResources(common);
}

TEST(StMgr, test_back_pressure_stmgr) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 18000;
  common.tmanager_controller_port_ = 18001;
  common.tmanager_stats_port_ = 18002;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 48000;
  common.ckptmgr_port_ = 58000;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(3);
  common.num_spouts_ = 1;
  common.num_spout_instances_ = 3;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 3;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";
  // Overwrite the default values for back pressure
  common.high_watermark_ = 1_MB;
  common.low_watermark_ = 500_KB;

  int num_msgs_sent_by_spout_instance = 500 * 1000 * 1000;  // 100M

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start two regular stmgrs and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss1;
  heron::stmgr::StMgr* regular_stmgr1 = NULL;
  std::thread* regular_stmgr_thread1 = NULL;

  StartStMgr(regular_stmgr_ss1, regular_stmgr1, regular_stmgr_thread1, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss1);

  std::shared_ptr<EventLoopImpl> regular_stmgr_ss2;
  heron::stmgr::StMgr* regular_stmgr2 = NULL;
  std::thread* regular_stmgr_thread2 = NULL;

  StartStMgr(regular_stmgr_ss2, regular_stmgr2, regular_stmgr_thread2, common.tmanager_host_,
             common.stmgr_ports_[1], common.local_data_ports_[1],
             common.topology_name_, common.topology_id_,
             common.topology_, common.stmgr_instance_id_list_[1], common.stmgrs_id_list_[1],
             common.zkhostportlist_, common.dpath_, common.metricsmgr_port_,
             common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);

  common.ss_list_.push_back(regular_stmgr_ss2);

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[2],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[2],
                  common.stmgr_instance_list_[2]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr1->GetPhysicalPlan()) sleep(1);

  // Stop regular stmgr2; at this point regular stmgr1 should send a start bp notification msg
  regular_stmgr_ss2->loopExit();

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStartBPMsgs() == 0) sleep(1);

  // Now kill the regular stmgr2, at this point regulat stmgr 1 should send a stop bp notification
  regular_stmgr_thread2->join();
  delete regular_stmgr2;
  delete regular_stmgr_thread2;

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStopBPMsgs() == 0) sleep(1);

  EXPECT_EQ(dummy_stmgr->NumStopBPMsgs(), 1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread1->join();
  dummy_stmgr_thread->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }
  // Delete the common resources
  delete regular_stmgr_thread1;
  delete regular_stmgr1;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  TearCommonResources(common);
}

TEST(StMgr, test_back_pressure_stmgr_reconnect) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 18500;
  common.tmanager_controller_port_ = 18501;
  common.tmanager_stats_port_ = 18502;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 49000;
  common.ckptmgr_port_ = 59000;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 4;
  common.num_spout_instances_ = 4;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";
  // Overwrite the default values for back pressure
  common.high_watermark_ = 1_MB;
  common.low_watermark_ = 500_KB;

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Stop the bolt schedulers at this point so that they stop receiving
  // This will build up back pressure
  for (size_t i = 0; i < common.bolt_workers_list_.size(); ++i) {
    common.bolt_workers_list_[i]->getEventLoop()->loopExit();
  }

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStartBPMsgs() == 0) sleep(1);

  // Now disconnect the dummy stmgr and reconnect; it should get the back pressure
  // notification again
  dummy_stmgr_ss->loopExit();
  dummy_stmgr_thread->join();
  delete dummy_stmgr_thread;
  delete dummy_stmgr;

  common.stmgr_ports_[1] = 0;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Wait till we get the back pressure notification
  while (dummy_stmgr->NumStartBPMsgs() == 0) sleep(1);

  EXPECT_EQ(dummy_stmgr->NumStartBPMsgs(), 1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  TearCommonResources(common);
}

TEST(StMgr, test_tmanager_restart_on_new_address) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 18500;
  common.tmanager_controller_port_ = 18501;
  common.tmanager_stats_port_ = 18502;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 49001;
  common.ckptmgr_port_ = 59001;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // A countdown latch to wait on, until metric mgr receives tmanager location
  // The count is 4 here, since we need to ensure it is sent twice for stmgr: once at
  // start, and once after receiving new tmanager location. Plus 2 from tmanager, total 4.
  // 5-4=1 is used to avoid countdown on 0
  CountDownLatch* metricsMgrTmanagerLatch = new CountDownLatch(5);

  // Start the metrics mgr, common.ss_list_[0]
  StartMetricsMgr(common, metricsMgrTmanagerLatch, NULL);

  // Start the tmanager etc. common.ss_list_[1]
  StartTManager(common);

  // Check the count: should be 5-1=4
  // The Tmanager sends its location to MetircsMgr when MetircsMgrClient initializes.
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(4, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(4), metricsMgrTmanagerLatch->getCount());

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  // common.ss_list_[2]
  common.ss_list_.push_back(regular_stmgr_ss);

  // Check the count: should be 4-1=3
  // The Stmgr sends Tmanager location to MetricsMgr when MetircsMgrClient initializes
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(3, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(3), metricsMgrTmanagerLatch->getCount());

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  // common.ss_list_[3]
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Kill current tmanager
  common.ss_list_[1]->loopExit();
  common.tmanager_thread_->join();
  delete common.tmanager_;
  delete common.tmanager_thread_;

  // Killing dummy stmgr so that we can restart it on another port, to change
  // the physical plan.
  dummy_stmgr_ss->loopExit();
  dummy_stmgr_thread->join();
  delete dummy_stmgr_thread;
  delete dummy_stmgr;

  // Change the tmanager port
  common.tmanager_port_ = 18511;

  // Start new dummy stmgr at different port, to generate a differnt pplan that we
  // can verify
  common.stmgr_ports_[1] = 0;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start tmanager on a different port
  StartTManager(common);

  // This confirms that metrics manager received the new tmanager location
  // Tmanager sends its location to MetricsMgr when MetricsMgrClient initialize: 3-1=2
  // Stmgr-0 watches new tmanager location and sends it to MetricsMgr: 2-1=1
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(1, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(1), metricsMgrTmanagerLatch->getCount());

  // Now wait until stmgr receives the new physical plan
  // No easy way to avoid sleep here.
  sleep(2);

  // Ensure that Stmgr connected to the new tmanager and has received new physical plan
  if (regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port() != common.stmgr_ports_[1]) {
    CHECK_EQ(regular_stmgr->GetPhysicalPlan()->stmgrs(0).data_port(), common.stmgr_ports_[1]);
  }

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  delete metricsMgrTmanagerLatch;
  TearCommonResources(common);
}

TEST(StMgr, test_tmanager_restart_on_same_address) {
  CommonResources common;

  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 18500;
  common.tmanager_controller_port_ = 18501;
  common.tmanager_stats_port_ = 18502;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 49002;
  common.ckptmgr_port_ = 59002;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // A countdown latch to wait on, until metric mgr receives tmanager location
  // The count is 2 here for stmgr, since we need to ensure it is sent twice: once at
  // start, and once after receiving new tmanager location
  CountDownLatch* metricsMgrTmanagerLatch = new CountDownLatch(5);

  // Start the metrics mgr
  StartMetricsMgr(common, metricsMgrTmanagerLatch, NULL);

  // Start the tmanager etc.
  StartTManager(common);

  // Check the count: should be 5-1=4
  // Tmanager send its location to MetricsMgr when MetricsMgrClient initializes
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(4, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(4), metricsMgrTmanagerLatch->getCount());

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Check the count: should be 4-1=3
  // Stmgr-0 sends tmanager location to MetrcisMgr when MetricsMgrClient initializes.
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(3, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(3), metricsMgrTmanagerLatch->getCount());

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Kill current tmanager
  common.ss_list_[1]->loopExit();
  common.tmanager_thread_->join();
  delete common.tmanager_;
  delete common.tmanager_thread_;

  // Killing dummy stmgr so that we can restart it on another port, to change
  // the physical plan.
  dummy_stmgr_ss->loopExit();
  dummy_stmgr_thread->join();
  delete dummy_stmgr_thread;
  delete dummy_stmgr;

  // Start new dummy stmgr at different port, to generate a differnt pplan that we
  // can verify
  sp_int32 stmgr_port_old = common.stmgr_ports_[1];
  common.stmgr_ports_[1] = 0;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start tmanager on a different port
  StartTManager(common);

  // This confirms that metrics manager received the new tmanager location
  // Check the count: should be 3-2=1
  // Tmanager sends its location when MetricsMgrClient initialize
  // Stmgr-0 watches and sends tmanager location
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(1, std::chrono::seconds(5)));
  EXPECT_EQ(static_cast<sp_uint32>(1), metricsMgrTmanagerLatch->getCount());

  // Now wait until stmgr receives the new physical plan.
  // No easy way to avoid sleep here.
  // Note: Here we sleep longer compared to the previous test as we need
  // to tmanagerClient could take upto 1 second (specified in test_heron_internals.yaml)
  // to retry connecting to tmanager.
  int retries = 30;
  while (regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port() == stmgr_port_old
         && retries--)
    sleep(1);

  // Ensure that Stmgr connected to the new tmanager and has received new physical plan
  CHECK_EQ(regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port(), common.stmgr_ports_[1]);
  CHECK_EQ(regular_stmgr->GetPhysicalPlan()->stmgrs(1).local_data_port(),
           common.local_data_ports_[1]);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  common.metrics_mgr_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  delete metricsMgrTmanagerLatch;
  TearCommonResources(common);
}

// This tests to make sure that metrics mgr upon reconnect
// will get the tmanager location
TEST(StMgr, test_metricsmgr_reconnect) {
  CommonResources common;
  // Initialize dummy params
  common.tmanager_host_ = LOCALHOST;
  common.tmanager_port_ = 19000;
  common.tmanager_controller_port_ = 19001;
  common.tmanager_stats_port_ = 19002;
  common.metricsmgr_port_ = 0;
  common.shell_port_ = 49500;
  common.ckptmgr_port_ = 59500;
  common.ckptmgr_id_ = "ckptmgr";
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.setNumStmgrs(2);
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // A countdown latch to wait on, until metric mgr receives tmanager location
  CountDownLatch* metricsMgrTmanagerLatch = new CountDownLatch(1);
  // A countdown latch to wait on metrics manager to close connnection.
  CountDownLatch* metricsMgrConnectionCloseLatch = new CountDownLatch(1);
  // Start the metrics mgr
  StartMetricsMgr(common, metricsMgrTmanagerLatch, metricsMgrConnectionCloseLatch);

  // lets remember this
  std::shared_ptr<EventLoopImpl> mmgr_ss = common.ss_list_.back();

  // Start the tmanager etc.
  StartTManager(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  std::shared_ptr<EventLoopImpl> regular_stmgr_ss;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.tmanager_host_,
             common.stmgr_ports_[0], common.local_data_ports_[0],
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_, common.ckptmgr_port_,
             common.ckptmgr_id_, common.high_watermark_, common.low_watermark_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  std::shared_ptr<EventLoopImpl> dummy_stmgr_ss;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_ports_[1],
                  common.tmanager_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // wait until metrics mgr also get time to get tmanager location
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(0, std::chrono::seconds(5)));

  // Check that metricsmgr got it
  VerifyMetricsMgrTManager(common);

  // Kill the metrics mgr
  for (auto iter = common.ss_list_.begin(); iter != common.ss_list_.end(); ++iter) {
    if (*iter == mmgr_ss) {
      common.ss_list_.erase(iter);
      break;
    }
  }

  // Exiting the loop without stopping, will not close the connection
  // between the stmgr and metrics mgr (since it is run a dummy thread
  // instead of an actual process). When a new metrics mgr comes up,
  // it will miss any register requests from stmgr, while stmgr keeps trying.
  // Hence expliciltly stopping to force connection close.
  common.metrics_mgr_->Stop();

  // Stopping the server will enqueue connnection cleanup callbacks
  // to the select server. To ensure they get executed gracefully,
  // need to wait until metrics mgr notifies us.
  EXPECT_TRUE(metricsMgrConnectionCloseLatch->wait(0, std::chrono::seconds(5)));
  mmgr_ss->loopExit();
  common.metrics_mgr_thread_->join();
  delete common.metrics_mgr_thread_;
  common.metrics_mgr_thread_ = NULL;
  delete common.metrics_mgr_;
  common.metrics_mgr_ = NULL;
  delete metricsMgrTmanagerLatch;
  delete metricsMgrConnectionCloseLatch;

  metricsMgrTmanagerLatch = new CountDownLatch(1);
  metricsMgrConnectionCloseLatch = new CountDownLatch(1);
  // Start the metrics mgr again
  StartMetricsMgr(common, metricsMgrTmanagerLatch, metricsMgrConnectionCloseLatch);
  EXPECT_TRUE(metricsMgrTmanagerLatch->wait(0, std::chrono::seconds(5)));

  // Check that metricsmgr got it
  VerifyMetricsMgrTManager(common);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmanager_thread_->join();
  regular_stmgr_thread->join();
  dummy_stmgr_thread->join();
  common.metrics_mgr_thread_->join();
  for (size_t i = 0; i < common.spout_workers_threads_list_.size(); ++i) {
    common.spout_workers_threads_list_[i]->join();
  }
  for (size_t i = 0; i < common.bolt_workers_threads_list_.size(); ++i) {
    common.bolt_workers_threads_list_[i]->join();
  }

  // Delete the common resources
  delete regular_stmgr_thread;
  delete regular_stmgr;
  delete dummy_stmgr_thread;
  delete dummy_stmgr;
  delete metricsMgrTmanagerLatch;
  delete metricsMgrConnectionCloseLatch;
  TearCommonResources(common);
}

// Test PatchPhysicalPlanWithHydratedTopology function
TEST(StMgr, test_PatchPhysicalPlanWithHydratedTopology) {
  int32_t nSpouts = 2;
  int32_t nSpoutInstances = 1;
  int32_t nBolts = 3;
  int32_t nBoltInstances = 1;
  heron::proto::api::Topology* topology =
      GenerateDummyTopology("topology_name",
                            "topology_id",
                            nSpouts, nSpoutInstances, nBolts, nBoltInstances,
                            heron::proto::api::SHUFFLE);

  heron::proto::system::PhysicalPlan* pplan = new heron::proto::system::PhysicalPlan();
  pplan->mutable_topology()->CopyFrom(*topology);

  // Verify initial values
  EXPECT_EQ(
    heron::config::TopologyConfigHelper::GetTopologyConfigValue(
        *topology,
        heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        ""),
    "30");
  EXPECT_EQ(
    heron::config::TopologyConfigHelper::GetTopologyConfigValue(
        pplan->topology(),
        heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        ""),
    "30");
  // Change runtime data in PhysicalPlan and patch it
  std::map<std::string, std::string> update;
  update["conf.new"] = "test";
  update[heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS] = "10";
  heron::config::TopologyConfigHelper::SetTopologyRuntimeConfig(pplan->mutable_topology(), update);

  // Verify updated runtime data is still in the patched physical plan
  // The topology in the physical plan should have the old name
  EXPECT_EQ(
    heron::config::TopologyConfigHelper::GetTopologyConfigValue(
        *topology,
        heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        ""),
    "30");  // The internal topology object should still have the initial value
  EXPECT_EQ(
    heron::config::TopologyConfigHelper::GetTopologyConfigValue(
        pplan->topology(),
        heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS,
        ""),
    "10");  // The topology object in the physical plan should have the new value
  EXPECT_EQ(
    heron::config::TopologyConfigHelper::GetTopologyConfigValue(
        pplan->topology(), "conf.new", ""),
    "test");  // The topology object in the physical plan should have the new config

  delete pplan;
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  std::cout << "Current working directory (to find stmgr logs) "
      << ProcessUtils::getCurrentWorkingDirectory() << std::endl;
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }
  return RUN_ALL_TESTS();
}
