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

#include "manager/tmanager.h"
#include <sys/resource.h>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <set>
#include <vector>
#include "manager/tmetrics-collector.h"
#include "manager/tcontroller.h"
#include "manager/stats-interface.h"
#include "manager/tmanagerserver.h"
#include "manager/stmgrstate.h"
#include "manager/stateful-controller.h"
#include "manager/ckptmgr-client.h"
#include "processor/processor.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "zookeeper/zkclient.h"
#include "config/helper.h"
#include "config/heron-internals-config-reader.h"
#include "statemgr/heron-statemgr.h"
#include "metrics/tmanager-metrics.h"

namespace heron {
namespace tmanager {

using std::unique_ptr;
using std::make_shared;

// Stats for the process
const sp_string METRIC_CPU_USER = "__cpu_user_usec";
const sp_string METRIC_CPU_SYSTEM = "__cpu_system_usec";
const sp_string METRIC_UPTIME = "__uptime_sec";
const sp_string METRIC_MEM_USED = "__mem_used_bytes";
const sp_int64 STATE_MANAGER_RETRY_FREQUENCY = 10_s;
const sp_int64 PROCESS_METRICS_FREQUENCY = 60_s;
const sp_int64 UPTIME_METRIC_FREQUENCY = 1_s;
const sp_string METRIC_PREFIX = "__process";

TManager::TManager(const std::string& _zk_hostport, const std::string& _topology_name,
                 const std::string& _topology_id, const std::string& _topdir,
                 sp_int32 _tmanager_controller_port,
                 sp_int32 _server_port, sp_int32 _stats_port, sp_int32 metricsMgrPort,
                 sp_int32 _ckptmgr_port,
                 const std::string& _metrics_sinks_yaml, const std::string& _myhost_name,
                 shared_ptr<EventLoop> eventLoop) {
  start_time_ = std::chrono::high_resolution_clock::now();
  zk_hostport_ = _zk_hostport;
  topdir_ = _topdir;
  tmanager_controller_ = nullptr;
  tmanager_controller_port_ = _tmanager_controller_port;
  server_ = nullptr;
  server_port_ = _server_port;
  stats_ = nullptr;
  stats_port_ = _stats_port;
  myhost_name_ = _myhost_name;
  eventLoop_ = eventLoop;
  dns_ = new AsyncDNS(eventLoop_);
  http_client_ = new HTTPClient(eventLoop_, dns_);

  metrics_collector_ = make_shared<TMetricsCollector>(config::HeronInternalsConfigReader::Instance()
                                         ->GetHeronTmanagerMetricsCollectorMaximumIntervalMin() * 60,
                                           eventLoop_, _metrics_sinks_yaml);

  mMetricsMgrPort = metricsMgrPort;

  sp_int32 metricsExportIntervalSec =
      config::HeronInternalsConfigReader::Instance()->GetHeronMetricsExportIntervalSec();

  mMetricsMgrClient = make_shared<heron::common::MetricsMgrSt>(
      mMetricsMgrPort, metricsExportIntervalSec, eventLoop_);
  mMetricsMgrClient->Start(myhost_name_, server_port_, "__tmanager__",
                           "0");  // MM expects task_id, so just giving 0 for tmanager.

  tmanagerProcessMetrics = make_shared<heron::common::MultiAssignableMetric>();
  mMetricsMgrClient->register_metric(METRIC_PREFIX, tmanagerProcessMetrics);

  ckptmgr_port_ = _ckptmgr_port;
  ckptmgr_client_ = nullptr;

  current_pplan_ = nullptr;

  // The topology as first submitted by the user
  // It shall only be used to construct the physical plan when TManager first time starts
  // Any runtime changes shall be made to current_pplan_->topology
  topology_ = nullptr;
  packing_plan_ = nullptr;
  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(zk_hostport_, topdir_, eventLoop_);

  assignment_in_progress_ = false;
  do_reassign_ = false;

  server_establish_attempts_ = 0;
  tmanager_location_ = make_unique<proto::tmanager::TManagerLocation>();
  tmanager_location_->set_topology_name(_topology_name);
  tmanager_location_->set_topology_id(_topology_id);
  tmanager_location_->set_host(myhost_name_);
  tmanager_location_->set_controller_port(tmanager_controller_port_);
  tmanager_location_->set_server_port(server_port_);
  tmanager_location_->set_stats_port(stats_port_);
  DCHECK(tmanager_location_->IsInitialized());
  FetchPackingPlan();

  // Send tmanager location to metrics mgr
  mMetricsMgrClient->RefreshTManagerLocation(*tmanager_location_);

  // Check for log pruning every 5 minutes
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::PruneLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingPruneIntervalSec() *
                   1_s),
           0);

  // Flush logs every interval
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::FlushLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingFlushIntervalSec() *
                   1_s),
           0);

  // Update uptime metric every 1 second
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateUptimeMetric();
  }, true, UPTIME_METRIC_FREQUENCY), 0);

  // Update Process related metrics every 60 seconds
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateProcessMetrics(status);
  }, true, PROCESS_METRICS_FREQUENCY), 0);

  stateful_controller_ = nullptr;
}

void TManager::FetchPackingPlan() {
  auto packing_plan = make_shared<proto::system::PackingPlan>();

  state_mgr_->GetPackingPlan(tmanager_location_->topology_name(), packing_plan,
                             [packing_plan, this](proto::system::StatusCode status) {
    this->OnPackingPlanFetch(packing_plan, status);
  });
}

void TManager::OnPackingPlanFetch(shared_ptr<proto::system::PackingPlan> newPackingPlan,
                                 proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    LOG(INFO) << "PackingPlan Fetch failed with status " << _status;
    LOG(INFO) << "Retrying after " << STATE_MANAGER_RETRY_FREQUENCY << " micro seconds ";
    CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status) {
      this->FetchPackingPlan();
    }, false, STATE_MANAGER_RETRY_FREQUENCY), 0);
  } else {
    // We got a new PackingPlan.
    LOG(INFO) << "Fetched PackingPlan of " << newPackingPlan->container_plans().size()
              << " containers.";

    if (!packing_plan_) {
      LOG(INFO) << "Received initial packing plan. Initializing absent_stmgrs_";
      packing_plan_ = newPackingPlan;
      // We will keep the list of stmgrs with us.
      // In case a assignment already exists, we will throw this
      // list out and re-init with whats in assignment.
      absent_stmgrs_.clear();
      for (sp_int32 i = 0; i < packing_plan_->container_plans_size(); ++i) {
        LOG(INFO) << "Adding container id " << packing_plan_->container_plans(i).id()
                  << " to absent_stmgrs_";
        // the packing plan represents container ids by the numerical id of the container. The
        // physical plan prepends "stmgr-" to the integer and represents it as a string.
        absent_stmgrs_.insert("stmgr-" + std::to_string(packing_plan_->container_plans(i).id()));
      }

      // this is part of the initialization process. Since we've got a packing plan we will
      // register our self as the server
      EstablishTManager(EventLoop::TIMEOUT_EVENT);

    } else {
      // We must know for sure that we are TManager before potentially deleting the physical plan
      // in state manager. We know this to be the case here because we initially fetch
      // packing_plan_ before becoming server, but we register the packing plan watcher only after
      // becoming server. That guarantees that if packing_plan_ is already set and this method is
      // invoked, it's due to the watch and we're server here.
      if (packing_plan_ != newPackingPlan) {
        LOG(INFO) << "Packing plan changed. Deleting physical plan and restarting TManager to "
                  << "reset internal state. Exiting.";
        state_mgr_->DeletePhysicalPlan(tmanager_location_->topology_name(),
          [](proto::system::StatusCode status) {
            ::exit(1);
          });
      } else {
        LOG(INFO) << "New Packing plan matches existing one.";
      }
    }
  }
}

void TManager::EstablishTManager(EventLoop::Status) {
  auto cb = [this](proto::system::StatusCode code) { this->SetTManagerLocationDone(code); };

  state_mgr_->SetTManagerLocation(*tmanager_location_, std::move(cb));

  // if zk lost the tmanager location, tmanager quits to bail out and re-establish its location
  auto cb2 = []() {
    LOG(ERROR) << " lost tmanager location in zk state manager. Bailing out..." << std::endl;
    ::exit(1);
  };
  state_mgr_->SetTManagerLocationWatch(tmanager_location_->topology_name(), std::move(cb2));

  server_establish_attempts_++;
}

TManager::~TManager() {
  if (server_) {
    server_->Stop();
  }

  for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
    stmgrs_.erase(iter->first);
  }

  stmgrs_.clear();

  mMetricsMgrClient->unregister_metric(METRIC_PREFIX);

  delete http_client_;
  delete dns_;
}

void TManager::UpdateUptimeMetric() {
  auto end_time = std::chrono::high_resolution_clock::now();
  auto uptime = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time_).count();
  tmanagerProcessMetrics->scope(METRIC_UPTIME)->SetValue(uptime);
}

void TManager::UpdateProcessMetrics(EventLoop::Status) {
  // CPU
  struct rusage usage;
  ProcessUtils::getResourceUsage(&usage);
  tmanagerProcessMetrics->scope(METRIC_CPU_USER)
      ->SetValue((usage.ru_utime.tv_sec * 1_s) + usage.ru_utime.tv_usec);
  tmanagerProcessMetrics->scope(METRIC_CPU_SYSTEM)
      ->SetValue((usage.ru_stime.tv_sec * 1_s) + usage.ru_stime.tv_usec);
  // Memory
  size_t totalmemory = ProcessUtils::getTotalMemoryUsed();
  tmanagerProcessMetrics->scope(METRIC_MEM_USED)->SetValue(totalmemory);
}

void TManager::SetTManagerLocationDone(proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    if (_code == proto::system::TMANAGERLOCATION_ALREADY_EXISTS &&
        server_establish_attempts_ <
            config::HeronInternalsConfigReader::Instance()->GetHeronTmanagerEstablishRetryTimes()) {
      LOG(INFO) << "Topology Manager node already exists. Maybe its "
                << "because of our restart. Will try again" << std::endl;
      // Attempt again
      auto cb = [this](EventLoop::Status status) { this->EstablishTManager(status); };
      eventLoop_->registerTimer(std::move(cb), false,
                                config::HeronInternalsConfigReader::Instance()
                                        ->GetHeronTmanagerEstablishRetryIntervalSec() *
                                    1_s);
      return;
    }
    // There was an error setting our location
    LOG(ERROR) << "For topology " << tmanager_location_->topology_name()
               << " Error setting ourselves as TManager. Error code is " << _code << std::endl;
    ::exit(1);
  }

  server_establish_attempts_ = 0;

  // We are now the server
  LOG(INFO) << "Successfully set ourselves as server\n";

  // Lets now read the topology
  topology_ = make_unique<proto::api::Topology>();
  state_mgr_->GetTopology(tmanager_location_->topology_name(), *topology_,
                          [this](proto::system::StatusCode code) {
    this->GetTopologyDone(code);
  });

  // and register packing plan watcher to pick up changes
  state_mgr_->SetPackingPlanWatch(tmanager_location_->topology_name(), [this]() {
    this->FetchPackingPlan();
  });
}

void TManager::GetTopologyDone(proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    // Without Topology we can't do much
    LOG(ERROR) << "For topology " << tmanager_location_->topology_name()
               << " Error getting topology. Error code is " << _code << std::endl;
    ::exit(1);
  }
  // Ok things are fine. topology_ contains the result
  // Just make sure it makes sense.
  CHECK_NOTNULL(topology_);

  if (!ValidateTopology(*topology_)) {
    LOG(ERROR) << "Topology invalid" << std::endl;
    ::exit(1);
  }
  LOG(INFO) << "Topology read and validated\n";

  if (heron::config::TopologyConfigHelper::GetReliabilityMode(*topology_)
      == config::TopologyConfigVars::EFFECTIVELY_ONCE) {
    // Establish connection to ckptmgr
    NetworkOptions ckpt_options;
    ckpt_options.set_host("127.0.0.1");
    ckpt_options.set_port(ckptmgr_port_);
    ckpt_options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                           ->GetHeronTmanagerNetworkServerOptionsMaximumPacketMb() *
                                       1024 * 1024);
    ckptmgr_client_ = make_unique<CkptMgrClient>(eventLoop_, ckpt_options,
                                        topology_->name(), topology_->id(),
                                        std::bind(&TManager::HandleCleanStatefulCheckpointResponse,
                                        this, std::placeholders::_1));
    // Start the client
    ckptmgr_client_->Start();

    // We also need to load latest checkpoint state
    auto ckpt = std::make_shared<proto::ckptmgr::StatefulConsistentCheckpoints>();
    auto cb = [ckpt, this](proto::system::StatusCode code) {
      this->GetStatefulCheckpointsDone(ckpt, code);
    };

    state_mgr_->GetStatefulCheckpoints(tmanager_location_->topology_name(), ckpt, std::move(cb));
  } else {
    // Now see if there is already a pplan
    FetchPhysicalPlan();
  }
}

void TManager::GetStatefulCheckpointsDone(
        shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt,
        proto::system::StatusCode _code) {
  if (_code != proto::system::OK && _code != proto::system::PATH_DOES_NOT_EXIST) {
    LOG(FATAL) << "For topology " << tmanager_location_->topology_name()
               << " Getting Stateful Checkpoint failed with error " << _code;
  }
  if (_code == proto::system::PATH_DOES_NOT_EXIST) {
    LOG(INFO) << "For topology " << tmanager_location_->topology_name()
              << " No existing globally consistent checkpoint found "
              << " inserting a empty one";
    // We need to set an empty one
    auto ckpts = make_shared<proto::ckptmgr::StatefulConsistentCheckpoints>();
    auto ckpt = ckpts->add_consistent_checkpoints();
    ckpt->set_checkpoint_id("");
    ckpt->set_packing_plan_id(packing_plan_->id());
    auto cb = [this, ckpts](proto::system::StatusCode code) {
      this->SetStatefulCheckpointsDone(code, ckpts);
    };

    state_mgr_->CreateStatefulCheckpoints(tmanager_location_->topology_name(),
                                          ckpts, std::move(cb));
  } else {
    LOG(INFO) << "For topology " << tmanager_location_->topology_name()
              << " An existing globally consistent checkpoint found "
              << _ckpt->DebugString();
    SetupStatefulController(std::move(_ckpt));
    FetchPhysicalPlan();
  }
}

void TManager::SetStatefulCheckpointsDone(proto::system::StatusCode _code,
                                 shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt) {
  if (_code != proto::system::OK) {
    LOG(FATAL) << "For topology " << tmanager_location_->topology_name()
               << " Setting empty Stateful Checkpoint failed with error " << _code;
  }
  SetupStatefulController(_ckpt);
  FetchPhysicalPlan();
}

void TManager::SetupStatefulController(
        shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt) {
  sp_int64 stateful_checkpoint_interval =
       config::TopologyConfigHelper::GetStatefulCheckpointIntervalSecsWithDefault(*topology_, 300);
  CHECK_GT(stateful_checkpoint_interval, 0);

  auto cb = [this](const proto::ckptmgr::StatefulConsistentCheckpoints& new_ckpts) {
    this->HandleStatefulCheckpointSave(new_ckpts);
  };
  // Instantiate the stateful restorer/coordinator
  stateful_controller_ = make_unique<StatefulController>(topology_->name(), _ckpt, state_mgr_,
                                                                start_time_, mMetricsMgrClient, cb);
  LOG(INFO) << "Starting timer to checkpoint state every "
            << stateful_checkpoint_interval << " seconds";
  CHECK_GT(eventLoop_->registerTimer(
               [this](EventLoop::Status) { this->SendCheckpointMarker(); }, true,
               stateful_checkpoint_interval * 1000 * 1000),
           0);
}

void TManager::ResetTopologyState(Connection* _conn, const std::string& _dead_stmgr,
                                 int32_t _dead_instance, const std::string& _reason) {
  LOG(INFO) << "Got a reset topology request with dead_stmgr " << _dead_stmgr
            << " dead_instance " << _dead_instance << " and reason " << _reason;
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    LOG(ERROR) << "The ResetTopology came from an unknown connection";
    return;
  }
  const std::string& stmgr = connection_to_stmgr_id_[_conn];
  LOG(INFO) << "The ResetTopology message came from stmgr " << stmgr;
  if (stateful_controller_ && absent_stmgrs_.empty()) {
    if (!stateful_controller_->RestoreInProgress()) {
      LOG(INFO) << "We are not in restore, hence we are starting Restore";
      stateful_controller_->StartRestore(stmgrs_, false);
    } else if (stateful_controller_->GotRestoreResponse(stmgr)) {
      // We are in Restore but we have already gotten response from this
      // stmgr. Maybe some other connections dropped. So start afresh
      LOG(INFO) << "We are in restore and have already received response from this stmgr";
      LOG(INFO) << "Restarting the restore";
      stateful_controller_->StartRestore(stmgrs_, false);
    } else {
      // So we can safely ignore this because the stmgr will later
      // send us a RestoreResponse when things get better
      LOG(INFO) << "We are in restore and have not yet received response from this stmgr";
    }
  }
}

void TManager::FetchPhysicalPlan() {
  auto pplan = make_shared<proto::system::PhysicalPlan>();
  auto cb = [pplan, this](proto::system::StatusCode code) {
    this->GetPhysicalPlanDone(pplan, code);
  };

  state_mgr_->GetPhysicalPlan(tmanager_location_->topology_name(), pplan, std::move(cb));
}

void TManager::SendCheckpointMarker() {
  if (!absent_stmgrs_.empty()) {
    LOG(INFO) << "Not sending checkpoint marker because not all stmgrs have connected to us";
    return;
  }
  stateful_controller_->StartCheckpoint(stmgrs_);
}

void TManager::HandleInstanceStateStored(const std::string& _checkpoint_id,
                                        const proto::system::Instance& _instance) {
  LOG(INFO) << "Got notification from stmgr that we saved checkpoint for task "
            << _instance.info().task_id() << " for checkpoint " << _checkpoint_id;
  if (stateful_controller_) {
    stateful_controller_->HandleInstanceStateStored(_checkpoint_id, packing_plan_->id(), _instance);
  }
}

void TManager::HandleRestoreTopologyStateResponse(Connection* _conn,
                                                 const std::string& _checkpoint_id,
                                                 int64_t _restore_txid,
                                                 proto::system::StatusCode _status) {
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    LOG(ERROR) << "Got HandleRestoreState Response from unknown connection "
               << _conn->getIPAddress() << " : " << _conn->getPort();
    return;
  }
  if (stateful_controller_) {
    stateful_controller_->HandleStMgrRestored(connection_to_stmgr_id_[_conn], _checkpoint_id,
                                              _restore_txid, _status, stmgrs_);
  }
}

void TManager::GetPhysicalPlanDone(shared_ptr<proto::system::PhysicalPlan> _pplan,
                                  proto::system::StatusCode _code) {
  // Physical plan need not exist. First check if some other error occurred.
  if (_code != proto::system::OK && _code != proto::system::PATH_DOES_NOT_EXIST) {
    // Something bad happened. Bail out!
    // TODO(kramasamy): This is not as bad as it seems. Maybe we can delete this assignment
    // and have a new assignment instead.
    LOG(ERROR) << "For topology " << tmanager_location_->topology_name()
               << " Error getting assignment. Error code is " << _code << std::endl;
    ::exit(1);
  }

  if (_code == proto::system::PATH_DOES_NOT_EXIST) {
    LOG(ERROR) << "There was no existing physical plan\n";
    // We never did assignment in the first place
  } else {
    LOG(INFO) << "There was an existing physical plan\n";
    CHECK_EQ(_code, proto::system::OK);
    current_pplan_ = _pplan;
    if (stateful_controller_) {
      stateful_controller_->RegisterNewPhysicalPlan(*current_pplan_);
    }
  }

  // Now that we have our state all setup, its time to start accepting requests
  // Port for the stmgrs to connect to
  NetworkOptions server_options;
  server_options.set_host(myhost_name_);
  server_options.set_port(server_port_);
  server_options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                         ->GetHeronTmanagerNetworkServerOptionsMaximumPacketMb() *
                                     1_MB);
  server_options.set_socket_family(PF_INET);
  server_ = make_unique<TManagerServer>(eventLoop_, server_options, metrics_collector_, this);

  sp_int32 retval = server_->Start();
  if (retval != SP_OK) {
    LOG(FATAL) << "Failed to start TManager Server with rcode: " << retval;
  }

  // Port for the scheduler to connect to
  NetworkOptions controller_options;
  controller_options.set_host(myhost_name_);
  controller_options.set_port(tmanager_controller_port_);
  controller_options.set_max_packet_size(
      config::HeronInternalsConfigReader::Instance()
          ->GetHeronTmanagerNetworkControllerOptionsMaximumPacketMb() *
      1_MB);
  controller_options.set_socket_family(PF_INET);
  tmanager_controller_ = make_unique<TController>(eventLoop_, controller_options, this);

  retval = tmanager_controller_->Start();
  if (retval != SP_OK) {
    LOG(FATAL) << "Failed to start TManager Controller Server with rcode: " << retval;
  }

  // Http port for stat queries
  NetworkOptions stats_options;
  if (config::HeronInternalsConfigReader::Instance()
          ->GetHeronTmanagerMetricsNetworkBindAllInterfaces()) {
    stats_options.set_host("0.0.0.0");
  } else {
    stats_options.set_host(myhost_name_);
  }
  stats_options.set_port(stats_port_);
  stats_options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                        ->GetHeronTmanagerNetworkStatsOptionsMaximumPacketMb() *
                                    1_MB);
  stats_options.set_socket_family(PF_INET);
  stats_ = make_unique<StatsInterface>(eventLoop_, stats_options, metrics_collector_, this);
}

void TManager::ActivateTopology(VCallback<proto::system::StatusCode> cb) {
  CHECK_EQ(current_pplan_->topology().state(), proto::api::PAUSED);
  DCHECK(current_pplan_->topology().IsInitialized());

  // Set the status
  auto new_pplan = make_shared<proto::system::PhysicalPlan>();
  new_pplan->CopyFrom(*current_pplan_);
  new_pplan->mutable_topology()->set_state(proto::api::RUNNING);

  auto callback = [new_pplan, this, cb](proto::system::StatusCode code) {
    cb(code);
    this->SetPhysicalPlanDone(new_pplan, code);
  };

  state_mgr_->SetPhysicalPlan(*new_pplan, std::move(callback));
}

void TManager::DeActivateTopology(VCallback<proto::system::StatusCode> cb) {
  CHECK_EQ(current_pplan_->topology().state(), proto::api::RUNNING);
  DCHECK(current_pplan_->topology().IsInitialized());

  // Set the status
  auto new_pplan = make_shared<proto::system::PhysicalPlan>();
  new_pplan->CopyFrom(*current_pplan_);
  new_pplan->mutable_topology()->set_state(proto::api::PAUSED);

  auto callback = [new_pplan, this, cb](proto::system::StatusCode code) {
    cb(code);
    this->SetPhysicalPlanDone(new_pplan, code);
  };

  state_mgr_->SetPhysicalPlan(*new_pplan, std::move(callback));
}

bool TManager::UpdateRuntimeConfig(const ComponentConfigMap& _config,
                                  VCallback<proto::system::StatusCode> cb) {
  DCHECK(current_pplan_->topology().IsInitialized());

  LOG(INFO) << "Update runtime config: ";
  LogConfig(_config);

  // Parse and set the new configs
  auto new_pplan = make_shared<proto::system::PhysicalPlan>();
  new_pplan->CopyFrom(*current_pplan_);
  proto::api::Topology* topology = new_pplan->mutable_topology();
  if (!UpdateRuntimeConfigInTopology(topology, _config)) {
    LOG(ERROR) << "Fail to update runtime config in topology";
    return false;
  }

  auto callback = [new_pplan, this, cb](proto::system::StatusCode code) {
    cb(code);
    this->SetPhysicalPlanDone(new_pplan, code);
  };

  state_mgr_->SetPhysicalPlan(*new_pplan, std::move(callback));
  return true;
}

void TManager::CleanAllStatefulCheckpoint() {
  ckptmgr_client_->SendCleanStatefulCheckpointRequest("", true);
}

void TManager::HandleStatefulCheckpointSave(
    const proto::ckptmgr::StatefulConsistentCheckpoints &new_ckpts) {
  // broadcast globally consistent checkpoint completion
  proto::ckptmgr::StatefulConsistentCheckpointSaved msg;
  msg.mutable_consistent_checkpoint()->CopyFrom(new_ckpts.consistent_checkpoints(0));

  for (auto & stmgr : stmgrs_) {
    stmgr.second->SendCheckpointSavedMessage(msg);
  }

  // clean oldest checkpoint on save
  std::string oldest_ckpt_id =
      new_ckpts.consistent_checkpoints(new_ckpts.consistent_checkpoints_size() - 1)
        .checkpoint_id();

  ckptmgr_client_->SendCleanStatefulCheckpointRequest(oldest_ckpt_id, false);
}

// Called when ckptmgr completes the clean stateful checkpoint request
void TManager::HandleCleanStatefulCheckpointResponse(proto::system::StatusCode _status) {
  tmanager_controller_->HandleCleanStatefulCheckpointResponse(_status);
}

// Update configurations in physical plan.
// Return false if a config doesn't exist, but this shouldn't happen if the config has been
// validated using ValidateRuntimeConig() function.
bool TManager::UpdateRuntimeConfigInTopology(proto::api::Topology* _topology,
                                            const ComponentConfigMap& _config) {
  DCHECK(_topology->IsInitialized());

  ComponentConfigMap::const_iterator iter;
  const char* topology_key = config::TopologyConfigHelper::GetReservedTopologyConfigKey();
  for (iter = _config.begin(); iter != _config.end(); ++iter) {
    // Get config for topology or component.
    std::map<std::string, std::string> config;
    config::TopologyConfigHelper::ConvertToRuntimeConfigs(iter->second, config);
    if (iter->first == topology_key) {
      config::TopologyConfigHelper::SetTopologyRuntimeConfig(_topology, config);
    } else {
      config::TopologyConfigHelper::SetComponentRuntimeConfig(_topology, iter->first, config);
    }
  }

  return true;
}

bool TManager::ValidateRuntimeConfig(const ComponentConfigMap& _config) const {
  return ValidateRuntimeConfigNames(_config);
}

void TManager::KillContainer(const std::string& host_name,
    sp_int32 shell_port, const std::string& stmgr_id) {
  LOG(INFO) << "Start killing " << stmgr_id << " on " <<
    host_name << ":" << shell_port;
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("secret", GetTopologyId()));
  OutgoingHTTPRequest* request =
    new OutgoingHTTPRequest(host_name, shell_port,
        "/killexecutor", BaseHTTPRequest::POST, kvs);
  auto cb = [host_name, shell_port, stmgr_id](IncomingHTTPResponse* response) {
    LOG(INFO) << "Response code of HTTP request of killing " << stmgr_id
      << " on " << host_name << ":" << shell_port << ": "
      << response->response_code();
  };
  if (http_client_->SendRequest(request, std::move(cb)) != SP_OK) {
    LOG(ERROR) << "Failed to kill " << stmgr_id << " on "
      << host_name << ":" << shell_port;
  }
  LOG(INFO) << "Finish killing " << stmgr_id << " on " <<
    host_name << ":" << shell_port;
  return;
}

proto::system::Status* TManager::RegisterStMgr(
                               const proto::system::StMgr& _stmgr,
                               const std::vector<shared_ptr<proto::system::Instance>>& _instances,
                               Connection* _conn, shared_ptr<proto::system::PhysicalPlan>& _pplan) {
  const std::string& stmgr_id = _stmgr.id();
  LOG(INFO) << "Got a register stream manager request from " << stmgr_id;

  // First check if there are any other stream manager present with the same id
  if (stmgrs_.find(stmgr_id) != stmgrs_.end()) {
    // Some other dude is already present with us.
    // First check to see if that other guy has timed out
    if (!stmgrs_[stmgr_id]->TimedOut()) {
      LOG(ERROR) << "Another stream manager " << stmgr_id << " exists at "
                 << stmgrs_[stmgr_id]->get_connection()->getIPAddress() << ":"
                 << stmgrs_[stmgr_id]->get_connection()->getPort()
                 << " with the same id and it hasn't timed out";
      LOG(INFO) << "Potential zombie host exists. Start killing both containers";
      std::string zombie_host_name = stmgrs_[stmgr_id]->get_stmgr()->host_name();
      sp_int32 zombie_port = stmgrs_[stmgr_id]->get_stmgr()->shell_port();
      std::string new_host_name = _stmgr.host_name();
      sp_int32 new_port = _stmgr.shell_port();
      KillContainer(zombie_host_name, zombie_port, stmgr_id);
      KillContainer(new_host_name, new_port, stmgr_id);
      proto::system::Status* status = new proto::system::Status();
      status->set_status(proto::system::DUPLICATE_STRMGR);
      status->set_message("Duplicate StreamManager");
      return status;
    } else {
      // The other guy has timed out
      // TODO(kramasamy): Currently, whenever a disconnect happens, we remove it
      // for the stmgrs_ list. Which means this case will only happen
      // if the stmgr maintains connection but hasn't sent a heartbeat
      // in a while.
      LOG(ERROR) << "Another stmgr exists at "
                 << stmgrs_[stmgr_id]->get_connection()->getIPAddress() << ":"
                 << stmgrs_[stmgr_id]->get_connection()->getPort()
                 << " with the same id but it has timed out";
      stmgrs_[stmgr_id]->UpdateWithNewStMgr(_stmgr, _instances, _conn);
      connection_to_stmgr_id_[_conn] = stmgr_id;
    }
  } else if (absent_stmgrs_.find(stmgr_id) == absent_stmgrs_.end()) {
    // Check to see if we were expecting this guy and it is not expected
    LOG(ERROR) << "We were not expecting stream manager " << stmgr_id;
    proto::system::Status* status = new proto::system::Status();
    status->set_status(proto::system::INVALID_STMGR);
    status->set_message("Invalid StreamManager");
    return status;
  } else {
    // This guy was indeed expected
    stmgrs_[stmgr_id] = make_shared<StMgrState>(_conn, _stmgr, _instances, *server_);
    connection_to_stmgr_id_[_conn] = stmgr_id;
    absent_stmgrs_.erase(stmgr_id);
  }

  if (absent_stmgrs_.empty()) {
    // All stmgrs are ready
    if (assignment_in_progress_) {
      do_reassign_ = true;
    } else {
      assignment_in_progress_ = true;
      LOG(INFO) << "All stream managers have connected with us";
      auto cb = [this](EventLoop::Status status) { this->DoPhysicalPlan(status); };
      CHECK_GE(eventLoop_->registerTimer(std::move(cb), false, 0), 0);
    }
  }

  _pplan = current_pplan_;
  proto::system::Status* status = new proto::system::Status();
  status->set_status(proto::system::OK);
  status->set_message("Welcome StreamManager");
  return status;
}

void TManager::DoPhysicalPlan(EventLoop::Status) {
  do_reassign_ = false;

  if (!absent_stmgrs_.empty()) {
    LOG(INFO) << "Called DoPhysicalPlan when absent_stmgrs_size is " << absent_stmgrs_.size()
              << std::endl;
    // Dont do anything.
    assignment_in_progress_ = false;
    return;
  }

  // TODO(kramasamy): If current_assignment exists, we need
  // to use as many portions from it as possible
  shared_ptr<proto::system::PhysicalPlan> pplan = MakePhysicalPlan();
  CHECK_NOTNULL(pplan);
  DCHECK(pplan->IsInitialized());

  if (!ValidateStMgrsWithPackingPlan()) {
    // TODO(kramasamy): Do Something better here
    LOG(ERROR) << "Packing plan and StMgr mismatch... Dying\n";
    ::exit(1);
  }

  auto cb = [pplan, this](proto::system::StatusCode code) {
    this->SetPhysicalPlanDone(pplan, code);
  };

  if (current_pplan_) {
    state_mgr_->SetPhysicalPlan(*pplan, std::move(cb));
  } else {
    // This is the first time a physical plan was made
    // TODO(kramasamy): This is probably not the right solution
    // because what if do_reassign_ is set to true before
    // SetPhysicalPlan returns. Then we will be doing another
    // Create which will fail. Fix it later.
    state_mgr_->CreatePhysicalPlan(*pplan, std::move(cb));
  }
}

void TManager::SetPhysicalPlanDone(shared_ptr<proto::system::PhysicalPlan> _pplan,
                                  proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    LOG(ERROR) << "Error writing assignment to statemgr. Error code is " << _code << std::endl;
    ::exit(1);
  }

  LOG(INFO) << "Successfully wrote new assignment to state" << std::endl;

  if (do_reassign_) {
    // Some other mapping change happened
    assignment_in_progress_ = true;
    LOG(INFO) << "Doing assignment since physical assignment might have changed" << std::endl;
    auto cb = [this](EventLoop::Status status) { this->DoPhysicalPlan(status); };
    CHECK_GE(eventLoop_->registerTimer(std::move(cb), false, 0), 0);
  } else {
    bool first_time_pplan = current_pplan_ == nullptr;
    current_pplan_ = _pplan;
    assignment_in_progress_ = false;
    // We need to pass that on to all streammanagers
    DistributePhysicalPlan();
    if (stateful_controller_) {
      stateful_controller_->RegisterNewPhysicalPlan(*current_pplan_);
      LOG(INFO) << "Starting Stateful Restore now that all stmgrs have connected";
      stateful_controller_->StartRestore(stmgrs_,
        first_time_pplan &&
        config::TopologyConfigHelper::StatefulTopologyStartClean(current_pplan_->topology()));
    }
  }
}

bool TManager::DistributePhysicalPlan() {
  if (current_pplan_) {
    // First valid the physical plan to distribute
    LOG(INFO) << "To distribute new physical plan:" << std::endl;
    config::PhysicalPlanHelper::LogPhysicalPlan(*current_pplan_);
    config::TopologyConfigHelper::LogTopology(current_pplan_->topology());

    // Distribute physical plan to all active stmgrs
    StMgrMapIter iter;
    for (iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
      iter->second->NewPhysicalPlan(*current_pplan_);
    }
    return true;
  }

  LOG(ERROR) << "No valid physical plan yet" << std::endl;
  return false;
}

std::unique_ptr<proto::tmanager::StmgrsRegistrationSummaryResponse> TManager::GetStmgrsRegSummary() {
  auto response = std::unique_ptr<proto::tmanager::StmgrsRegistrationSummaryResponse>(
          new proto::tmanager::StmgrsRegistrationSummaryResponse());

  for (auto it = stmgrs_.begin(); it != stmgrs_.end(); ++it) {
    response->add_registered_stmgrs(it->first);
  }

  for (auto it = absent_stmgrs_.begin(); it != absent_stmgrs_.end(); ++it) {
    response->add_absent_stmgrs(*it);
  }

  return response;
}

shared_ptr<proto::system::PhysicalPlan> TManager::MakePhysicalPlan() {
  // TODO(kramasamy): At some point, we need to talk to our scheduler
  // and do this scheduling
  if (current_pplan_) {
    // There is already an existing assignment. However stmgrs might have
    // died and come up on different machines. This means that
    // we need to just adjust the stmgrs mapping
    // First lets verify that our original pplan and instances
    // all match up
    CHECK(ValidateStMgrsWithPhysicalPlan(current_pplan_));
    auto new_pplan = make_shared<proto::system::PhysicalPlan>();
    new_pplan->mutable_topology()->CopyFrom(current_pplan_->topology());

    for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
      new_pplan->add_stmgrs()->CopyFrom(*(iter->second->get_stmgr()));
    }
    for (sp_int32 i = 0; i < current_pplan_->instances_size(); ++i) {
      new_pplan->add_instances()->CopyFrom(current_pplan_->instances(i));
    }

    return new_pplan;
  }

  // TManager does not really have any control over who does what.
  // That has already been decided while launching the jobs.
  // TManager just stitches the info together to pass to everyone

  // Build the PhysicalPlan structure
  auto new_pplan = make_shared<proto::system::PhysicalPlan>();
  new_pplan->mutable_topology()->CopyFrom(*topology_);

  // Build the physical assignments
  for (StMgrMapIter stmgr_iter = stmgrs_.begin(); stmgr_iter != stmgrs_.end(); ++stmgr_iter) {
    new_pplan->add_stmgrs()->CopyFrom(*(stmgr_iter->second->get_stmgr()));
    const std::vector<shared_ptr<proto::system::Instance>>& instances =
            stmgr_iter->second->get_instances();
    for (size_t i = 0; i < instances.size(); ++i) {
      new_pplan->add_instances()->CopyFrom(*(instances[i]));
    }
  }

  return new_pplan;
}

proto::system::Status* TManager::UpdateStMgrHeartbeat(Connection* _conn, sp_int64 _time,
                                                     proto::system::StMgrStats* _stats) {
  proto::system::Status* retval = new proto::system::Status();
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    retval->set_status(proto::system::INVALID_STMGR);
    retval->set_message("Unknown connection doing stmgr heartbeat");
    return retval;
  }
  const std::string& stmgr = connection_to_stmgr_id_[_conn];
  // TODO(kramasamy): Maybe do more checks?
  if (stmgrs_.find(stmgr) == stmgrs_.end()) {
    retval->set_status(proto::system::INVALID_STMGR);
    retval->set_message("Unknown stream manager id");
    return retval;
  }
  if (stmgrs_[stmgr]->get_connection() != _conn) {
    retval->set_status(proto::system::INVALID_STMGR);
    retval->set_message("Unknown stream manager connection");
    return retval;
  }
  stmgrs_[stmgr]->heartbeat(_time, _stats);
  retval->set_status(proto::system::OK);
  return retval;
}

proto::system::StatusCode TManager::RemoveStMgrConnection(Connection* _conn) {
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    return proto::system::INVALID_STMGR;
  }
  const std::string& stmgr_id = connection_to_stmgr_id_[_conn];
  if (stmgrs_.find(stmgr_id) == stmgrs_.end()) {
    return proto::system::INVALID_STMGR;
  }
  auto stmgr = stmgrs_[stmgr_id];
  // This guy disconnected from us
  LOG(INFO) << "StMgr " << stmgr->get_stmgr()->id() << " disconnected from us" << std::endl;
  stmgrs_.erase(stmgr->get_id());
  connection_to_stmgr_id_.erase(_conn);
  absent_stmgrs_.insert(stmgr->get_id());
  return proto::system::OK;
}

////////////////////////////////////////////////////////////////////////////////
// Below are valid checking functions
////////////////////////////////////////////////////////////////////////////////
bool TManager::ValidateTopology(const proto::api::Topology& _topology) {
  if (tmanager_location_->topology_name() != _topology.name()) {
    LOG(ERROR) << "topology name mismatch! Expected topology name is "
               << tmanager_location_->topology_name() << " but found in zk " << _topology.name()
               << std::endl;
    return false;
  }
  if (tmanager_location_->topology_id() != _topology.id()) {
    LOG(ERROR) << "topology id mismatch! Expected topology id is "
               << tmanager_location_->topology_id() << " but found in zk " << _topology.id()
               << std::endl;
    return false;
  }
  std::set<std::string> component_names;
  for (sp_int32 i = 0; i < _topology.spouts_size(); ++i) {
    if (component_names.find(_topology.spouts(i).comp().name()) != component_names.end()) {
      LOG(ERROR) << "Component names are not unique " << _topology.spouts(i).comp().name() << "\n";
      return false;
    }
    component_names.insert(_topology.spouts(i).comp().name());
  }

  for (sp_int32 i = 0; i < _topology.bolts_size(); ++i) {
    if (component_names.find(_topology.bolts(i).comp().name()) != component_names.end()) {
      LOG(ERROR) << "Component names are not unique " << _topology.bolts(i).comp().name() << "\n";
      return false;
    }
    component_names.insert(_topology.bolts(i).comp().name());
  }

  return true;
}

bool TManager::ValidateStMgrsWithPackingPlan() {
  // here we check to see if the total number of instances
  // across all stmgrs match up to all the spout/bolt
  // parallelism the packing plan has specified
  sp_int32 ntasks = 0;
  for (sp_int32 i = 0; i < packing_plan_->container_plans_size(); ++i) {
    ntasks += packing_plan_->container_plans(i).instance_plans_size();
  }

  sp_int32 ninstances = 0;
  for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
    ninstances += iter->second->get_num_instances();
  }

  return ninstances == ntasks;
}

bool TManager::ValidateStMgrsWithPhysicalPlan(shared_ptr<proto::system::PhysicalPlan> _pplan) {
  std::map<std::string, std::vector<proto::system::Instance*> > stmgr_to_instance_map;
  for (sp_int32 i = 0; i < _pplan->instances_size(); ++i) {
    proto::system::Instance* instance = _pplan->mutable_instances(i);
    if (stmgr_to_instance_map.find(instance->stmgr_id()) == stmgr_to_instance_map.end()) {
      std::vector<proto::system::Instance*> instances;
      instances.push_back(instance);
      stmgr_to_instance_map[instance->stmgr_id()] = instances;
    } else {
      stmgr_to_instance_map[instance->stmgr_id()].push_back(instance);
    }
  }
  for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
    if (stmgr_to_instance_map.find(iter->first) == stmgr_to_instance_map.end()) {
      LOG(ERROR) << "Instances info from " << iter->first
                 << " is missing. Bailing out..." << std::endl;
      return false;
    }
    if (!iter->second->VerifyInstances(stmgr_to_instance_map[iter->first])) {
      LOG(ERROR) << "Instances verification failed for " << iter->first
                 << ". Bailing out..." << std::endl;
      return false;
    }
  }
  return true;
}

/**
 * Make sure component names exist
 */
bool TManager::ValidateRuntimeConfigNames(const ComponentConfigMap& _config) const {
  LOG(INFO) << "Validating runtime configs.";
  const proto::api::Topology& topology = current_pplan_->topology();
  DCHECK(topology.IsInitialized());

  std::unordered_set<std::string> components;
  config::TopologyConfigHelper::GetAllComponentNames(topology, components);

  ComponentConfigMap::const_iterator iter;
  const char* topology_key = config::TopologyConfigHelper::GetReservedTopologyConfigKey();
  for (iter = _config.begin(); iter != _config.end(); ++iter) {
    if (iter->first != topology_key) {
      // It is a component, search for it
      if (components.find(iter->first) == components.end()) {
        return false;
      }
    }
  }

  return true;
}

void TManager::LogConfig(const ComponentConfigMap& _config) {
  for (auto iter = _config.begin(); iter != _config.end(); ++iter) {
    LOG(INFO) << iter->first << " =>";
    for (auto i = iter->second.begin(); i != iter->second.end(); ++i) {
      LOG(INFO) << i->first << " : " << i->second;
    }
  }
}

}  // namespace tmanager
}  // namespace heron
