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
#include "manager/tmaster.h"
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
void CreateLocalStateOnFS(heron::proto::api::Topology* topology, sp_string dpath) {
  EventLoopImpl ss;

  // Write the dummy topology/tmaster location out to the local file system
  // via thestate mgr
  heron::common::HeronLocalFileStateMgr state_mgr(dpath, &ss);
  state_mgr.CreateTopology(*topology, NULL);
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
                  sp_int32 tmaster_port, sp_int32 tmaster_controller_port,
                  sp_int32 tmaster_stats_port, sp_int32 metrics_mgr_port) {
  ss = new EventLoopImpl();
  tmaster =
      new heron::tmaster::TMaster(zkhostportlist, topology_name, topology_id, dpath, stmgrs_id_list,
                                  tmaster_controller_port, tmaster_port, tmaster_stats_port,
                                  metrics_mgr_port, metrics_sinks_config_filename, LOCALHOST, ss);
  tmaster_thread = new std::thread(StartServer, ss);
}

void StartStMgr(EventLoopImpl*& ss, heron::stmgr::StMgr*& mgr, std::thread*& stmgr_thread,
                sp_int32 stmgr_port, const sp_string& topology_name, const sp_string& topology_id,
                const heron::proto::api::Topology* topology, const std::vector<sp_string>& workers,
                const sp_string& stmgr_id, const sp_string& zkhostportlist, const sp_string& dpath,
                sp_int32 metricsmgr_port, sp_int32 shell_port) {
  // The topology will be owned and deleted by the strmgr
  heron::proto::api::Topology* stmgr_topology = new heron::proto::api::Topology();
  stmgr_topology->CopyFrom(*topology);
  // Create the select server for this stmgr to use
  ss = new EventLoopImpl();
  mgr =
      new heron::stmgr::StMgr(ss, stmgr_port, topology_name, topology_id, stmgr_topology, stmgr_id,
                              workers, zkhostportlist, dpath, metricsmgr_port, shell_port);
  mgr->Init();
  stmgr_thread = new std::thread(StartServer, ss);
}

void StartDummyStMgr(EventLoopImpl*& ss, DummyStMgr*& mgr, std::thread*& stmgr_thread,
                     sp_int32 stmgr_port, sp_int32 tmaster_port, sp_int32 shell_port,
                     const sp_string& stmgr_id,
                     const std::vector<heron::proto::system::Instance*>& instances) {
  // Create the select server for this stmgr to use
  ss = new EventLoopImpl();

  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  mgr = new DummyStMgr(ss, options, stmgr_id, LOCALHOST, stmgr_port, LOCALHOST, tmaster_port,
                       shell_port, instances);
  mgr->Start();
  stmgr_thread = new std::thread(StartServer, ss);
}

void StartDummyMtrMgr(EventLoopImpl*& ss, DummyMtrMgr*& mgr, std::thread*& mtmgr_thread,
                      sp_int32 mtmgr_port, const sp_string& stmgr_id, CountDownLatch* tmasterLatch,
                      CountDownLatch* connectionCloseLatch) {
  // Create the select server for this stmgr to use
  ss = new EventLoopImpl();

  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(mtmgr_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  mgr = new DummyMtrMgr(ss, options, stmgr_id, tmasterLatch, connectionCloseLatch);
  mgr->Start();
  mtmgr_thread = new std::thread(StartServer, ss);
}

void StartDummySpoutInstance(EventLoopImpl*& ss, DummySpoutInstance*& worker,
                             std::thread*& worker_thread, sp_int32 stmgr_port,
                             const sp_string& topology_name, const sp_string& topology_id,
                             const sp_string& instance_id, const sp_string& component_name,
                             sp_int32 task_id, sp_int32 component_index, const sp_string& stmgr_id,
                             const sp_string& stream_id, sp_int32 max_msgs_to_send,
                             bool _do_custom_grouping) {
  // Create the select server for this worker to use
  ss = new EventLoopImpl();
  // Create the network option
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  worker = new DummySpoutInstance(ss, options, topology_name, topology_id, instance_id,
                                  component_name, task_id, component_index, stmgr_id, stream_id,
                                  max_msgs_to_send, _do_custom_grouping);
  worker->Start();
  worker_thread = new std::thread(StartServer, ss);
}

void StartDummyBoltInstance(EventLoopImpl*& ss, DummyBoltInstance*& worker,
                            std::thread*& worker_thread, sp_int32 stmgr_port,
                            const sp_string& topology_name, const sp_string& topology_id,
                            const sp_string& instance_id, const sp_string& component_name,
                            sp_int32 task_id, sp_int32 component_index, const sp_string& stmgr_id,
                            sp_int32 expected_msgs_to_recv) {
  // Create the select server for this worker to use
  ss = new EventLoopImpl();
  // Create the network option
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(stmgr_port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  worker =
      new DummyBoltInstance(ss, options, topology_name, topology_id, instance_id, component_name,
                            task_id, component_index, stmgr_id, expected_msgs_to_recv);
  worker->Start();
  worker_thread = new std::thread(StartServer, ss);
}

struct CommonResources {
  // arguments
  sp_int32 tmaster_port_;
  sp_int32 tmaster_controller_port_;
  sp_int32 tmaster_stats_port_;
  sp_int32 stmgr_baseport_;
  sp_int32 metricsmgr_port_;
  sp_int32 shell_port_;
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

    // Create a temporary directory to write out the state
    char dpath[255];
    snprintf(dpath, sizeof(dpath), "%s", "/tmp/XXXXXX");
    mkdtemp(dpath);
    dpath_ = sp_string(dpath);
  }
};

void StartTMaster(CommonResources& common) {
  // Generate a dummy topology
  common.topology_ = GenerateDummyTopology(
      common.topology_name_, common.topology_id_, common.num_spouts_, common.num_spout_instances_,
      common.num_bolts_, common.num_bolt_instances_, common.grouping_);

  // Create the zk state on the local file system
  CreateLocalStateOnFS(common.topology_, common.dpath_);

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
               common.tmaster_port_, common.tmaster_controller_port_, common.tmaster_stats_port_,
               common.metricsmgr_port_ + 1);
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

void StartDummySpoutInstanceHelper(CommonResources& common, sp_int8 spout, sp_int8 spout_instance,
                                   sp_int32 num_msgs_sent_by_spout_instance) {
  // Start the spout
  EventLoopImpl* worker_ss = NULL;
  DummySpoutInstance* worker = NULL;
  std::thread* worker_thread = NULL;
  sp_string streamid = STREAM_NAME;
  streamid += std::to_string(spout);
  sp_string instanceid = CreateInstanceId(spout, spout_instance, true);
  StartDummySpoutInstance(worker_ss, worker, worker_thread,
                          common.stmgr_baseport_ + common.instanceid_stmgr_[instanceid],
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
  for (int bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      sp_string instanceid = CreateInstanceId(bolt, bolt_instance, false);
      if (common.instanceid_instance_[instanceid]->info().task_id() < min_bolt_task_id) {
        min_bolt_task_id = common.instanceid_instance_[instanceid]->info().task_id();
      }
    }
  }

  // Start the spouts
  for (int spout = 0; spout < common.num_spouts_; ++spout) {
    for (int spout_instance = 0; spout_instance < common.num_spout_instances_; ++spout_instance) {
      StartDummySpoutInstanceHelper(common, spout, spout_instance, num_msgs_sent_by_spout_instance);
    }
  }
  // Start the bolts
  std::vector<DummyBoltInstance*> bolt_workers_list;
  std::vector<std::thread*> bolt_workers_threads_list;
  for (int bolt = 0; bolt < common.num_bolts_; ++bolt) {
    for (int bolt_instance = 0; bolt_instance < common.num_bolt_instances_; ++bolt_instance) {
      EventLoopImpl* worker_ss = NULL;
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
                             common.stmgr_baseport_ + common.instanceid_stmgr_[instanceid],
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
  for (int i = 0; i < common.num_stmgrs_; ++i) {
    EventLoopImpl* stmgr_ss = NULL;
    heron::stmgr::StMgr* mgr = NULL;
    std::thread* stmgr_thread = NULL;
    StartStMgr(stmgr_ss, mgr, stmgr_thread, common.stmgr_baseport_ + i, common.topology_name_,
               common.topology_id_, common.topology_, common.stmgr_instance_id_list_[i],
               common.stmgrs_id_list_[i], common.zkhostportlist_, common.dpath_,
               common.metricsmgr_port_, common.shell_port_);

    common.ss_list_.push_back(stmgr_ss);
    common.stmgrs_list_.push_back(mgr);
    common.stmgrs_threads_list_.push_back(stmgr_thread);
  }
}

void StartMetricsMgr(CommonResources& common, CountDownLatch* tmasterLatch,
                     CountDownLatch* connectionCloseLatch) {
  EventLoopImpl* ss = NULL;
  DummyMtrMgr* mgr = NULL;
  std::thread* metrics_mgr = NULL;
  StartDummyMtrMgr(ss, mgr, metrics_mgr, common.metricsmgr_port_, "stmgr", tmasterLatch,
                   connectionCloseLatch);
  common.ss_list_.push_back(ss);
  common.metrics_mgr_ = mgr;
  common.metrics_mgr_thread_ = metrics_mgr;
}

void StartMetricsMgr(CommonResources& common) { StartMetricsMgr(common, NULL, NULL); }

void TearCommonResources(CommonResources& common) {
  delete common.topology_;
  delete common.tmaster_thread_;
  delete common.tmaster_;
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

  for (size_t i = 0; i < common.ss_list_.size(); ++i) delete common.ss_list_[i];

  for (auto itr = common.instanceid_instance_.begin();
       itr != common.instanceid_instance_.end(); ++itr)
    delete itr->second;

  // Clean up the local filesystem state
  FileUtils::removeRecursive(common.dpath_, true);
}

void VerifyMetricsMgrTMaster(CommonResources& common) {
  EXPECT_NE(common.metrics_mgr_->get_tmaster(), (heron::proto::tmaster::TMasterLocation*)NULL);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->topology_name(), common.topology_name_);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->topology_id(), common.topology_id_);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->host(), LOCALHOST);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->controller_port(), common.tmaster_controller_port_);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->master_port(), common.tmaster_port_);
  EXPECT_EQ(common.metrics_mgr_->get_tmaster()->stats_port(), common.tmaster_stats_port_);
}

// Test to make sure that the stmgr can decode the pplan
TEST(StMgr, test_pplan_decode) {
  CommonResources common;
  // Initialize dummy params
  common.tmaster_port_ = 10000;
  common.tmaster_controller_port_ = 10001;
  common.tmaster_stats_port_ = 10002;
  common.stmgr_baseport_ = 20000;
  common.metricsmgr_port_ = 30000;
  common.shell_port_ = 40000;
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

  sp_int8 num_workers_per_stmgr_ = (((common.num_spouts_ * common.num_spout_instances_) +
                                     (common.num_bolts_ * common.num_bolt_instances_)) /
                                    common.num_stmgrs_);
  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // Start the stream managers
  StartStMgrs(common);

  // Start dummy worker to make the stmgr connect to the tmaster
  StartWorkerComponents(common, 0, 0);

  // Wait till we get the physical plan populated on atleast one of the stmgrs
  // We just pick the first one
  while (!common.stmgrs_list_[0]->GetPhysicalPlan()) sleep(1);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
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

// Test to make sure that the stmgr can route data properly
TEST(StMgr, test_tuple_route) {
  CommonResources common;

  // Initialize dummy params
  common.tmaster_port_ = 15000;
  common.tmaster_controller_port_ = 15001;
  common.tmaster_stats_port_ = 15002;
  common.stmgr_baseport_ = 25000;
  common.metricsmgr_port_ = 35000;
  common.shell_port_ = 45000;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 1;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 4;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

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
  common.tmaster_thread_->join();
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
  common.tmaster_port_ = 15500;
  common.tmaster_controller_port_ = 15501;
  common.tmaster_stats_port_ = 15502;
  common.stmgr_baseport_ = 25500;
  common.metricsmgr_port_ = 35500;
  common.shell_port_ = 45500;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 1;  // This test will only work with 1 type of spout
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 4;
  common.grouping_ = heron::proto::api::CUSTOM;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

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
  common.tmaster_thread_->join();
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
  common.tmaster_port_ = 17000;
  common.tmaster_controller_port_ = 17001;
  common.tmaster_stats_port_ = 17002;
  common.stmgr_baseport_ = 27000;
  common.metricsmgr_port_ = 37000;
  common.shell_port_ = 47000;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;

  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
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
  common.tmaster_thread_->join();
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
  common.tmaster_port_ = 17300;
  common.tmaster_controller_port_ = 17301;
  common.tmaster_stats_port_ = 17302;
  common.stmgr_baseport_ = 27300;
  common.metricsmgr_port_ = 37300;
  common.shell_port_ = 47300;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 1;
  common.num_spout_instances_ = 2;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
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
  common.tmaster_thread_->join();
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
  common.tmaster_port_ = 18000;
  common.tmaster_controller_port_ = 18001;
  common.tmaster_stats_port_ = 18002;
  common.stmgr_baseport_ = 28000;
  common.metricsmgr_port_ = 38000;
  common.shell_port_ = 48000;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 3;
  common.num_spouts_ = 1;
  common.num_spout_instances_ = 3;
  common.num_bolts_ = 1;
  common.num_bolt_instances_ = 3;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 500 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 1 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 500 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start two regular stmgrs and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss1 = NULL;
  heron::stmgr::StMgr* regular_stmgr1 = NULL;
  std::thread* regular_stmgr_thread1 = NULL;

  StartStMgr(regular_stmgr_ss1, regular_stmgr1, regular_stmgr_thread1, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss1);

  EventLoopImpl* regular_stmgr_ss2 = NULL;
  heron::stmgr::StMgr* regular_stmgr2 = NULL;
  std::thread* regular_stmgr_thread2 = NULL;
  StartStMgr(regular_stmgr_ss2, regular_stmgr2, regular_stmgr_thread2, common.stmgr_baseport_ + 1,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[1], common.stmgrs_id_list_[1], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss2);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 2,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[2],
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
  common.tmaster_thread_->join();
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
  common.tmaster_port_ = 18500;
  common.tmaster_controller_port_ = 18501;
  common.tmaster_stats_port_ = 18502;
  common.stmgr_baseport_ = 28500;
  common.metricsmgr_port_ = 39000;
  common.shell_port_ = 49000;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // Start the metrics mgr
  StartMetricsMgr(common);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
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

  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 2,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
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
  common.tmaster_thread_->join();
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

TEST(StMgr, test_tmaster_restart_on_new_address) {
  CommonResources common;

  // Initialize dummy params
  common.tmaster_port_ = 18500;
  common.tmaster_controller_port_ = 18501;
  common.tmaster_stats_port_ = 18502;
  common.stmgr_baseport_ = 28500;
  common.metricsmgr_port_ = 39001;
  common.shell_port_ = 49001;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // A countdown latch to wait on, until metric mgr receives tmaster location
  // The count is 2 here, since we need to ensure it is sent twice: once at
  // start, and once after receiving new tmaster location
  CountDownLatch* metricsMgrTmasterLatch = new CountDownLatch(2);

  // Start the metrics mgr
  StartMetricsMgr(common, metricsMgrTmasterLatch, NULL);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Kill current tmaster
  common.ss_list_.front()->loopExit();
  common.tmaster_thread_->join();
  delete common.tmaster_;
  delete common.tmaster_thread_;

  // Killing dummy stmgr so that we can restart it on another port, to change
  // the physical plan.
  dummy_stmgr_ss->loopExit();
  dummy_stmgr_thread->join();
  delete dummy_stmgr_thread;
  delete dummy_stmgr;

  // Change the tmaster port
  common.tmaster_port_ = 18511;

  // Start new dummy stmgr at different port, to generate a differnt pplan that we
  // can verify
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 2,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start tmaster on a different port
  StartTMaster(common);

  // This confirms that metrics manager received the new tmaster location
  metricsMgrTmasterLatch->wait();

  // Now wait until stmgr receives the new physical plan
  // No easy way to avoid sleep here.
  sleep(2);

  // Ensure that Stmgr connected to the new tmaster and has received new physical plan
  CHECK_EQ(regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port(), common.stmgr_baseport_ + 2);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
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
  delete metricsMgrTmasterLatch;
  TearCommonResources(common);
}

TEST(StMgr, test_tmaster_restart_on_same_address) {
  CommonResources common;

  // Initialize dummy params
  common.tmaster_port_ = 18500;
  common.tmaster_controller_port_ = 18501;
  common.tmaster_stats_port_ = 18502;
  common.stmgr_baseport_ = 28500;
  common.metricsmgr_port_ = 39002;
  common.shell_port_ = 49002;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // A countdown latch to wait on, until metric mgr receives tmaster location
  // The count is 2 here, since we need to ensure it is sent twice: once at
  // start, and once after receiving new tmaster location
  CountDownLatch* metricsMgrTmasterLatch = new CountDownLatch(2);

  // Start the metrics mgr
  StartMetricsMgr(common, metricsMgrTmasterLatch, NULL);

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // Kill current tmaster
  common.ss_list_.front()->loopExit();
  common.tmaster_thread_->join();
  delete common.tmaster_;
  delete common.tmaster_thread_;

  // Killing dummy stmgr so that we can restart it on another port, to change
  // the physical plan.
  dummy_stmgr_ss->loopExit();
  dummy_stmgr_thread->join();
  delete dummy_stmgr_thread;
  delete dummy_stmgr;

  // Start new dummy stmgr at different port, to generate a differnt pplan that we
  // can verify
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 2,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start tmaster on a different port
  StartTMaster(common);

  // This confirms that metrics manager received the new tmaster location
  metricsMgrTmasterLatch->wait();

  // Now wait until stmgr receives the new physical plan.
  // No easy way to avoid sleep here.
  // Note: Here we sleep longer compared to the previous test as we need
  // to tmasterClient could take upto 1 second (specified in test_heron_internals.yaml)
  // to retry connecting to tmaster.
  int retries = 30;
  while (regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port() == common.stmgr_baseport_ + 1
         && retries--)
    sleep(1);

  // Ensure that Stmgr connected to the new tmaster and has received new physical plan
  CHECK_EQ(regular_stmgr->GetPhysicalPlan()->stmgrs(1).data_port(), common.stmgr_baseport_ + 2);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
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
  delete metricsMgrTmasterLatch;
  TearCommonResources(common);
}

// This tests to make sure that metrics mgr upon reconnect
// will get the tmaster location
TEST(StMgr, test_metricsmgr_reconnect) {
  CommonResources common;
  // Initialize dummy params
  common.tmaster_port_ = 19000;
  common.tmaster_controller_port_ = 19001;
  common.tmaster_stats_port_ = 19002;
  common.stmgr_baseport_ = 29000;
  common.metricsmgr_port_ = 39500;
  common.shell_port_ = 49500;
  common.topology_name_ = "mytopology";
  common.topology_id_ = "abcd-9999";
  common.num_stmgrs_ = 2;
  common.num_spouts_ = 2;
  common.num_spout_instances_ = 1;
  common.num_bolts_ = 2;
  common.num_bolt_instances_ = 1;
  common.grouping_ = heron::proto::api::SHUFFLE;
  // Empty so that we don't attempt to connect to the zk
  // but instead connect to the local filesytem
  common.zkhostportlist_ = "";

  int num_msgs_sent_by_spout_instance = 100 * 1000 * 1000;  // 100M

  // Lets change the Connection buffer HWM and LWN for back pressure to get the
  // test case done faster
  Connection::systemHWMOutstandingBytes = 10 * 1024 * 1024;
  Connection::systemLWMOutstandingBytes = 5 * 1024 * 1024;

  // Start the tmaster etc.
  StartTMaster(common);

  // A countdown latch to wait on, until metric mgr receives tmaster location
  CountDownLatch* metricsMgrTmasterLatch = new CountDownLatch(1);
  // A countdown latch to wait on metrics manager to close connnection.
  CountDownLatch* metricsMgrConnectionCloseLatch = new CountDownLatch(1);
  // Start the metrics mgr
  StartMetricsMgr(common, metricsMgrTmasterLatch, metricsMgrConnectionCloseLatch);

  // lets remember this
  EventLoopImpl* mmgr_ss = common.ss_list_.back();

  // Distribute workers across stmgrs
  DistributeWorkersAcrossStmgrs(common);

  // We'll start one regular stmgr and one dummy stmgr
  EventLoopImpl* regular_stmgr_ss = NULL;
  heron::stmgr::StMgr* regular_stmgr = NULL;
  std::thread* regular_stmgr_thread = NULL;
  StartStMgr(regular_stmgr_ss, regular_stmgr, regular_stmgr_thread, common.stmgr_baseport_,
             common.topology_name_, common.topology_id_, common.topology_,
             common.stmgr_instance_id_list_[0], common.stmgrs_id_list_[0], common.zkhostportlist_,
             common.dpath_, common.metricsmgr_port_, common.shell_port_);
  common.ss_list_.push_back(regular_stmgr_ss);

  // Start a dummy stmgr
  EventLoopImpl* dummy_stmgr_ss = NULL;
  DummyStMgr* dummy_stmgr = NULL;
  std::thread* dummy_stmgr_thread = NULL;
  StartDummyStMgr(dummy_stmgr_ss, dummy_stmgr, dummy_stmgr_thread, common.stmgr_baseport_ + 1,
                  common.tmaster_port_, common.shell_port_, common.stmgrs_id_list_[1],
                  common.stmgr_instance_list_[1]);
  common.ss_list_.push_back(dummy_stmgr_ss);

  // Start the dummy workers
  StartWorkerComponents(common, num_msgs_sent_by_spout_instance, num_msgs_sent_by_spout_instance);

  // Wait till we get the physical plan populated on the stmgr. That way we know the
  // workers have connected
  while (!regular_stmgr->GetPhysicalPlan()) sleep(1);

  // wait until metrics mgr also get time to get tmaster location
  metricsMgrTmasterLatch->wait();

  // Check that metricsmgr got it
  VerifyMetricsMgrTMaster(common);

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
  metricsMgrConnectionCloseLatch->wait();
  mmgr_ss->loopExit();
  common.metrics_mgr_thread_->join();
  delete common.metrics_mgr_thread_;
  common.metrics_mgr_thread_ = NULL;
  delete common.metrics_mgr_;
  common.metrics_mgr_ = NULL;
  delete metricsMgrTmasterLatch;
  delete metricsMgrConnectionCloseLatch;

  metricsMgrTmasterLatch = new CountDownLatch(1);
  metricsMgrConnectionCloseLatch = new CountDownLatch(1);
  // Start the metrics mgr again
  StartMetricsMgr(common, metricsMgrTmasterLatch, metricsMgrConnectionCloseLatch);
  metricsMgrTmasterLatch->wait();

  // Check that metricsmgr got it
  VerifyMetricsMgrTMaster(common);

  // Stop the schedulers
  for (size_t i = 0; i < common.ss_list_.size(); ++i) {
    common.ss_list_[i]->loopExit();
  }

  // Wait for the threads to terminate
  common.tmaster_thread_->join();
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
  delete metricsMgrTmasterLatch;
  delete metricsMgrConnectionCloseLatch;
  TearCommonResources(common);
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  if (argc > 1) {
    std::cerr << "Using config file " << argv[1] << std::endl;
    heron_internals_config_filename = argv[1];
  }
  return RUN_ALL_TESTS();
}
