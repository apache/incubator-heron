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

#include "manager/stmgr.h"
#include <sys/resource.h>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>
#include <utility>
#include "manager/stmgr-clientmgr.h"
#include "manager/stmgr-server.h"
#include "manager/stream-consumers.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "basics/mempool.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"
#include "config/helper.h"
#include "statemgr/heron-statemgr.h"
#include "metrics/metrics.h"
#include "metrics/metrics-mgr-st.h"
#include "util/xor-manager.h"
#include "util/neighbour-calculator.h"
#include "manager/stateful-restorer.h"
#include "manager/tmaster-client.h"
#include "util/tuple-cache.h"
#include "manager/ckptmgr-client.h"

namespace heron {
namespace stmgr {

// Stats for the process
const sp_string METRIC_CPU_USER = "__cpu_user_usec";
const sp_string METRIC_CPU_SYSTEM = "__cpu_system_usec";
const sp_string METRIC_UPTIME = "__uptime_sec";
const sp_string METRIC_MEM_USED = "__mem_used_bytes";
const sp_string RESTORE_DROPPED_STMGR_BYTES = "__stmgr_dropped_bytes";
const sp_string RESTORE_DROPPED_INSTANCE_TUPLES = "__instance_dropped_tuples";
const sp_int64 PROCESS_METRICS_FREQUENCY = 10_s;
const sp_int64 TMASTER_RETRY_FREQUENCY = 10_s;

StMgr::StMgr(EventLoop* eventLoop, const sp_string& _myhost, sp_int32 _myport,
             const sp_string& _topology_name, const sp_string& _topology_id,
             proto::api::Topology* _hydrated_topology, const sp_string& _stmgr_id,
             const std::vector<sp_string>& _instances, const sp_string& _zkhostport,
             const sp_string& _zkroot, sp_int32 _metricsmgr_port, sp_int32 _shell_port,
             sp_int32 _ckptmgr_port, const sp_string& _ckptmgr_id,
             sp_int64 _high_watermark, sp_int64 _low_watermark)

    : pplan_(NULL),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      stmgr_host_(_myhost),
      stmgr_port_(_myport),
      instances_(_instances),
      server_(NULL),
      clientmgr_(NULL),
      tmaster_client_(NULL),
      eventLoop_(eventLoop),
      xor_mgrs_(NULL),
      tuple_cache_(NULL),
      hydrated_topology_(_hydrated_topology),
      start_time_(std::chrono::high_resolution_clock::now()),
      zkhostport_(_zkhostport),
      zkroot_(_zkroot),
      metricsmgr_port_(_metricsmgr_port),
      shell_port_(_shell_port),
      ckptmgr_port_(_ckptmgr_port),
      ckptmgr_id_(_ckptmgr_id),
      high_watermark_(_high_watermark),
      low_watermark_(_low_watermark) {}

void StMgr::Init() {
  LOG(INFO) << "Init Stmgr" << std::endl;
  sp_int32 metrics_export_interval_sec =
      config::HeronInternalsConfigReader::Instance()->GetHeronMetricsExportIntervalSec();
  __global_protobuf_pool_set_pool_max_number_of_messages__(
    heron::config::HeronInternalsConfigReader::Instance()
      ->GetHeronStreammgrMempoolMaxMessageNumber());
  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(zkhostport_, zkroot_, eventLoop_, false);
  metrics_manager_client_ = new heron::common::MetricsMgrSt(
      stmgr_host_, stmgr_port_, metricsmgr_port_, "__stmgr__", stmgr_id_,
      metrics_export_interval_sec, eventLoop_);
  stmgr_process_metrics_ = new heron::common::MultiAssignableMetric();
  metrics_manager_client_->register_metric("__process", stmgr_process_metrics_);
  restore_initiated_metrics_ = new heron::common::CountMetric();
  metrics_manager_client_->register_metric("__restore_initiated", restore_initiated_metrics_);
  dropped_during_restore_metrics_ = new heron::common::MultiCountMetric();
  metrics_manager_client_->register_metric("__dropped_during_restore",
                                           dropped_during_restore_metrics_);
  state_mgr_->SetTMasterLocationWatch(topology_name_, [this]() { this->FetchTMasterLocation(); });
  state_mgr_->SetMetricsCacheLocationWatch(
                       topology_name_, [this]() { this->FetchMetricsCacheLocation(); });

  reliability_mode_ = heron::config::TopologyConfigHelper::GetReliabilityMode(*hydrated_topology_);
  if (reliability_mode_ == config::TopologyConfigVars::EXACTLY_ONCE) {
    // Start checkpoint manager client
    CreateCheckpointMgrClient();
  } else {
    ckptmgr_client_ = nullptr;
  }

  // Create the client manager
  clientmgr_ = new StMgrClientMgr(eventLoop_, topology_name_, topology_id_, stmgr_id_, this,
                                  metrics_manager_client_, high_watermark_, low_watermark_);

  // Create and Register Tuple cache
  CreateTupleCache();

  FetchTMasterLocation();
  FetchMetricsCacheLocation();

  CHECK_GT(
      eventLoop_->registerTimer(
          [this](EventLoop::Status status) { this->CheckTMasterLocation(status); }, false,
          config::HeronInternalsConfigReader::Instance()->GetCheckTMasterLocationIntervalSec() *
              1_s),
      0);  // fire only once

  // Instantiate neighbour calculator. Required by stmgr server
  neighbour_calculator_ = new NeighbourCalculator();

  // Create and start StmgrServer
  StartStmgrServer();

  if (reliability_mode_ == config::TopologyConfigVars::EXACTLY_ONCE) {
    // Now start the stateful restorer
    stateful_restorer_ = new StatefulRestorer(ckptmgr_client_, clientmgr_,
                               tuple_cache_, server_, metrics_manager_client_,
                               std::bind(&StMgr::HandleStatefulRestoreDone, this,
                                         std::placeholders::_1, std::placeholders::_2,
                                         std::placeholders::_3));
  } else {
    stateful_restorer_ = nullptr;
  }

  // Check for log pruning every 5 minutes
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::PruneLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingPruneIntervalSec() *
                   1_s),
           0);

  // Check for log flushing every 10 seconds
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::FlushLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingFlushIntervalSec() *
                   1_s),
           0);

  // Update Process related metrics every 10 seconds
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateProcessMetrics(status);
  }, true, PROCESS_METRICS_FREQUENCY), 0);

  is_acking_enabled =
        reliability_mode_ == config::TopologyConfigVars::TopologyReliabilityMode::ATLEAST_ONCE;
}

StMgr::~StMgr() {
  metrics_manager_client_->unregister_metric("__process");
  metrics_manager_client_->unregister_metric("__restore_initiated");
  metrics_manager_client_->unregister_metric("__dropped_during_restore");
  delete stmgr_process_metrics_;
  delete restore_initiated_metrics_;
  delete dropped_during_restore_metrics_;
  delete tuple_cache_;
  delete state_mgr_;
  delete pplan_;
  delete server_;
  delete clientmgr_;
  delete tmaster_client_;
  CleanupStreamConsumers();
  CleanupXorManagers();
  delete hydrated_topology_;
  delete ckptmgr_client_;
  delete stateful_restorer_;
  delete metrics_manager_client_;

  delete neighbour_calculator_;
}

bool StMgr::DidAnnounceBackPressure() { return server_->DidAnnounceBackPressure(); }

void StMgr::CheckTMasterLocation(EventLoop::Status) {
  if (!tmaster_client_) {
    LOG(FATAL) << "Could not fetch the TMaster location in time. Exiting. ";
  }
}

void StMgr::UpdateProcessMetrics(EventLoop::Status) {
  // Uptime
  auto end_time = std::chrono::high_resolution_clock::now();
  auto delta = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time_).count();
  stmgr_process_metrics_->scope(METRIC_UPTIME)->SetValue(delta);

  // CPU
  struct rusage usage;
  ProcessUtils::getResourceUsage(&usage);
  stmgr_process_metrics_->scope(METRIC_CPU_USER)
      ->SetValue((usage.ru_utime.tv_sec * 1_s) + usage.ru_utime.tv_usec);
  stmgr_process_metrics_->scope(METRIC_CPU_SYSTEM)
      ->SetValue((usage.ru_stime.tv_sec * 1_s) + usage.ru_stime.tv_usec);
  // Memory
  size_t totalmemory = ProcessUtils::getTotalMemoryUsed();
  stmgr_process_metrics_->scope(METRIC_MEM_USED)->SetValue(totalmemory);
}

void StMgr::FetchTMasterLocation() {
  LOG(INFO) << "Fetching TMaster Location";
  auto tmaster = new proto::tmaster::TMasterLocation();

  auto cb = [tmaster, this](proto::system::StatusCode status) {
    this->OnTMasterLocationFetch(tmaster, status);
  };

  state_mgr_->GetTMasterLocation(topology_name_, tmaster, std::move(cb));
}

void StMgr::FetchMetricsCacheLocation() {
  LOG(INFO) << "Fetching MetricsCache Location";
  auto metricscache = new proto::tmaster::MetricsCacheLocation();

  auto cb = [metricscache, this](proto::system::StatusCode status) {
    this->OnMetricsCacheLocationFetch(metricscache, status);
  };

  state_mgr_->GetMetricsCacheLocation(topology_name_, metricscache, std::move(cb));
}

void StMgr::StartStmgrServer() {
  CHECK(!server_);
  LOG(INFO) << "Creating StmgrServer" << std::endl;
  NetworkOptions sops;
  sops.set_host(IpUtils::getHostName());
  sops.set_port(stmgr_port_);
  sops.set_socket_family(PF_INET);
  sops.set_max_packet_size(
      config::HeronInternalsConfigReader::Instance()
          ->GetHeronStreammgrNetworkOptionsMaximumPacketMb() *
      1_MB);
  sops.set_high_watermark(high_watermark_);
  sops.set_low_watermark(low_watermark_);
  server_ = new StMgrServer(eventLoop_, sops, topology_name_, topology_id_, stmgr_id_, instances_,
                            this, metrics_manager_client_, neighbour_calculator_);

  // start the server
  CHECK_EQ(server_->Start(), 0);
}

void StMgr::CreateCheckpointMgrClient() {
  LOG(INFO) << "Creating CheckpointMgr Client at " << stmgr_host_ << ":" << ckptmgr_port_;
  NetworkOptions client_options;
  client_options.set_host("localhost");
  client_options.set_port(ckptmgr_port_);
  client_options.set_socket_family(PF_INET);
  client_options.set_max_packet_size(std::numeric_limits<sp_uint32>::max() - 1);
  auto save_watcher = std::bind(&StMgr::HandleSavedInstanceState, this,
                           std::placeholders::_1, std::placeholders::_2);
  auto get_watcher = std::bind(&StMgr::HandleGetInstanceState, this,
                           std::placeholders::_1, std::placeholders::_2,
                           std::placeholders::_3, std::placeholders::_4);
  auto ckpt_watcher = std::bind(&StMgr::HandleCkptMgrRegistration, this);
  ckptmgr_client_ = new CkptMgrClient(eventLoop_, client_options,
                                      topology_name_, topology_id_,
                                      ckptmgr_id_, stmgr_id_,
                                      save_watcher, get_watcher, ckpt_watcher);
  ckptmgr_client_->Start();
}

void StMgr::CreateTMasterClient(proto::tmaster::TMasterLocation* tmasterLocation) {
  CHECK(!tmaster_client_);
  LOG(INFO) << "Creating Tmaster Client at " << tmasterLocation->host() << ":"
            << tmasterLocation->master_port();
  NetworkOptions master_options;
  master_options.set_host(tmasterLocation->host());
  master_options.set_port(tmasterLocation->master_port());
  master_options.set_socket_family(PF_INET);
  master_options.set_max_packet_size(
      config::HeronInternalsConfigReader::Instance()
          ->GetHeronTmasterNetworkMasterOptionsMaximumPacketMb() *
      1_MB);
  master_options.set_high_watermark(high_watermark_);
  master_options.set_low_watermark(low_watermark_);
  auto pplan_watch = [this](proto::system::PhysicalPlan* pplan) { this->NewPhysicalPlan(pplan); };
  auto stateful_checkpoint_watch =
       [this](sp_string checkpoint_id) {
    this->InitiateStatefulCheckpoint(checkpoint_id);
  };
  auto restore_topology_watch =
       [this](sp_string checkpoint_id, sp_int64 restore_txid) {
    this->RestoreTopologyState(checkpoint_id, restore_txid);
  };
  auto start_stateful_watch =
       [this](sp_string checkpoint_id) {
    this->StartStatefulProcessing(checkpoint_id);
  };

  tmaster_client_ = new TMasterClient(eventLoop_, master_options, stmgr_id_, stmgr_host_,
                                      stmgr_port_, shell_port_, std::move(pplan_watch),
                                      std::move(stateful_checkpoint_watch),
                                      std::move(restore_topology_watch),
                                      std::move(start_stateful_watch));
}

void StMgr::CreateTupleCache() {
  CHECK(!tuple_cache_);
  LOG(INFO) << "Creating tuple cache ";
  sp_uint32 drain_threshold_bytes_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrCacheDrainSizeMb() * 1_MB;
  tuple_cache_ = new TupleCache(eventLoop_, drain_threshold_bytes_);

  tuple_cache_->RegisterDrainer(&StMgr::DrainInstanceData, this);
  tuple_cache_->RegisterCheckpointDrainer(&StMgr::DrainDownstreamCheckpoint, this);
}

void StMgr::HandleNewTmaster(proto::tmaster::TMasterLocation* newTmasterLocation) {
  // Lets delete the existing tmaster if we have one.
  if (tmaster_client_) {
    LOG(INFO) << "Destroying existing tmasterClient";
    tmaster_client_->Die();
    tmaster_client_ = NULL;
  }

  // Create the tmaster and the servers/clients but don't start the tmaster
  // connection as yet. We'll do that once we connect to all the instances.
  CreateTMasterClient(newTmasterLocation);

  // In the case where we are doing a tmaster refresh we may have already
  // connected to all of the instances
  if (server_ && server_->HaveAllInstancesConnectedToUs()) {
    StartTMasterClient();
  }
}

void StMgr::BroadcastTmasterLocation(proto::tmaster::TMasterLocation* tmasterLocation) {
  // Notify metrics manager of the tmaster location changes
  // TODO(vikasr): What if the refresh fails?
  metrics_manager_client_->RefreshTMasterLocation(*tmasterLocation);
}

void StMgr::BroadcastMetricsCacheLocation(proto::tmaster::MetricsCacheLocation* tmasterLocation) {
  // Notify metrics manager of the metricscache location changes
  // TODO(huijun): What if the refresh fails?
  LOG(INFO) << "BroadcastMetricsCacheLocation";
  metrics_manager_client_->RefreshMetricsCacheLocation(*tmasterLocation);
}

void StMgr::OnTMasterLocationFetch(proto::tmaster::TMasterLocation* newTmasterLocation,
                                   proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    LOG(INFO) << "TMaster Location Fetch failed with status " << _status;
    LOG(INFO) << "Retrying after " << TMASTER_RETRY_FREQUENCY << " micro seconds ";
    CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status) {
      this->FetchTMasterLocation();
    }, false, TMASTER_RETRY_FREQUENCY), 0);
  } else {
    // We got a new tmaster location.
    // Just verify that we are talking to the right entity
    if (newTmasterLocation->topology_name() != topology_name_ ||
        newTmasterLocation->topology_id() != topology_id_) {
      LOG(FATAL) << "Topology name/id mismatch between stmgr and TMaster "
                 << "We expected " << topology_name_ << " : " << topology_id_ << " but tmaster had "
                 << newTmasterLocation->topology_name() << " : "
                 << newTmasterLocation->topology_id();
    }

    LOG(INFO) << "Fetched TMasterLocation to be " << newTmasterLocation->host() << ":"
              << newTmasterLocation->master_port();

    bool isNewTmaster = true;

    if (tmaster_client_) {
      sp_string currentTmasterHostPort = tmaster_client_->getTmasterHostPort();
      std::string newTmasterHostPort =
          newTmasterLocation->host() + ":" + std::to_string(newTmasterLocation->master_port());

      if (currentTmasterHostPort == newTmasterHostPort) {
        LOG(INFO) << "New tmaster location same as the current one. "
                  << "Nothing to do here... ";
        isNewTmaster = false;
      } else {
        LOG(INFO) << "New tmaster location different from the current one."
                  << " Current one at " << currentTmasterHostPort << " and New one at "
                  << newTmasterHostPort;
        isNewTmaster = true;
      }
    }

    if (isNewTmaster) {
      HandleNewTmaster(newTmasterLocation);
    }

    // Stmgr doesn't know what other things might have changed, so it is important
    // to broadcast the location, even though we know its the same tmaster.
    BroadcastTmasterLocation(newTmasterLocation);
  }

  // Delete the tmasterLocation Proto
  delete newTmasterLocation;
}

void StMgr::OnMetricsCacheLocationFetch(proto::tmaster::MetricsCacheLocation* newTmasterLocation,
                                   proto::system::StatusCode _status) {
  if (_status != proto::system::OK) {
    LOG(INFO) << "MetricsCache Location Fetch failed with status " << _status;
    LOG(INFO) << "Retrying after " << TMASTER_RETRY_FREQUENCY << " micro seconds ";
    CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status) {
      this->FetchMetricsCacheLocation();
    }, false, TMASTER_RETRY_FREQUENCY), 0);
  } else {
    // We got a new metricscache location.
    // Just verify that we are talking to the right entity
    if (newTmasterLocation->topology_name() != topology_name_ ||
        newTmasterLocation->topology_id() != topology_id_) {
      LOG(FATAL) << "Topology name/id mismatch between stmgr and MetricsCache "
                 << "We expected " << topology_name_ << " : " << topology_id_
                 << " but MetricsCache had "
                 << newTmasterLocation->topology_name() << " : "
                 << newTmasterLocation->topology_id() << std::endl;
    }

    LOG(INFO) << "Fetched MetricsCacheLocation to be " << newTmasterLocation->host() << ":"
              << newTmasterLocation->master_port();

    // Stmgr doesn't know what other things might have changed, so it is important
    // to broadcast the location, even though we know its the same metricscache.
    BroadcastMetricsCacheLocation(newTmasterLocation);
  }

  // Delete the tmasterLocation Proto
  delete newTmasterLocation;
}

// Start the tmaster client
void StMgr::StartTMasterClient() {
  if (!tmaster_client_) {
    LOG(INFO) << "We haven't received tmaster location yet"
              << ", so tmaster_client_ hasn't been created"
              << "Once we get the location, it will be started";
    // Nothing else to do here
  } else {
    std::vector<proto::system::Instance*> all_instance_info;
    server_->GetInstanceInfo(all_instance_info);
    tmaster_client_->SetInstanceInfo(all_instance_info);
    if (!tmaster_client_->IsConnected()) {
      LOG(INFO) << "Connecting to the TMaster as all the instances have connected to us";
      tmaster_client_->Start();
    }
  }
}

void StMgr::NewPhysicalPlan(proto::system::PhysicalPlan* _pplan) {
  LOG(INFO) << "Received a new physical plan from tmaster";
  // first make sure that we are part of the plan ;)
  bool found = false;
  for (sp_int32 i = 0; i < _pplan->stmgrs_size(); ++i) {
    if (_pplan->stmgrs(i).id() == stmgr_id_) {
      found = true;
      break;
    }
  }

  if (!found) {
    LOG(FATAL) << "We have no role in this topology!!";
  }

  // The Topology structure here is not hydrated.
  // We need to hydrate it now.
  // Its possible that the topology's state might have changed.
  // So we need to handle it seperately.
  if (!pplan_) {
    LOG(INFO) << "This is the first time we received the physical plan";
  } else if (_pplan->topology().state() != pplan_->topology().state()) {
    LOG(INFO) << "Topology state changed from " << _pplan->topology().state() << " to "
              << pplan_->topology().state();
  }
  proto::api::TopologyState st = _pplan->topology().state();
  _pplan->clear_topology();
  _pplan->mutable_topology()->CopyFrom(*hydrated_topology_);
  _pplan->mutable_topology()->set_state(st);

  // TODO(vikasr) Currently we dont check if our role has changed

  // Build out data structures
  std::map<sp_string, std::vector<sp_int32> > component_to_task_ids;
  task_id_to_stmgr_.clear();
  for (sp_int32 i = 0; i < _pplan->instances_size(); ++i) {
    sp_int32 task_id = _pplan->instances(i).info().task_id();
    task_id_to_stmgr_[task_id] = _pplan->instances(i).stmgr_id();
    const sp_string& component_name = _pplan->instances(i).info().component_name();
    if (component_to_task_ids.find(component_name) == component_to_task_ids.end()) {
      component_to_task_ids[component_name] = std::vector<sp_int32>();
    }
    component_to_task_ids[component_name].push_back(task_id);
  }
  if (!pplan_) {
    PopulateStreamConsumers(_pplan->mutable_topology(), component_to_task_ids);
    PopulateXorManagers(_pplan->topology(), ExtractTopologyTimeout(_pplan->topology()),
                        component_to_task_ids);
  }

  delete pplan_;
  pplan_ = _pplan;
  neighbour_calculator_->Reconstruct(*pplan_);
  // For exactly once topologies, we only start connecting after we have recovered
  // from a globally consistent checkpoint. The act of starting connections is initiated
  // by the restorer
  if (!stateful_restorer_) {
    clientmgr_->StartConnections(pplan_);
  }
  server_->BroadcastNewPhysicalPlan(*pplan_);
}

void StMgr::CleanupStreamConsumers() {
  for (auto iter = stream_consumers_.begin(); iter != stream_consumers_.end(); ++iter) {
    delete iter->second;
  }
  stream_consumers_.clear();
}

void StMgr::CleanupXorManagers() {
  delete xor_mgrs_;
  xor_mgrs_ = NULL;
}

sp_int32 StMgr::ExtractTopologyTimeout(const proto::api::Topology& _topology) {
  for (sp_int32 i = 0; i < _topology.topology_config().kvs_size(); ++i) {
    if (_topology.topology_config().kvs(i).key() == "topology.message.timeout.secs") {
      return atoi(_topology.topology_config().kvs(i).value().c_str());
    }
  }
  LOG(FATAL) << "topology.message.timeout.secs does not exist";
  return 0;
}

void StMgr::PopulateStreamConsumers(
    proto::api::Topology* _topology,
    const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids) {
  // First get a map of <component, stream> -> Schema
  std::map<std::pair<sp_string, sp_string>, proto::api::StreamSchema*> schema_map;
  for (sp_int32 i = 0; i < _topology->spouts_size(); ++i) {
    for (sp_int32 j = 0; j < _topology->spouts(i).outputs_size(); ++j) {
      proto::api::OutputStream* os = _topology->mutable_spouts(i)->mutable_outputs(j);
      std::pair<sp_string, sp_string> p =
          make_pair(os->stream().component_name(), os->stream().id());
      schema_map[p] = os->mutable_schema();
    }
  }
  for (sp_int32 i = 0; i < _topology->bolts_size(); ++i) {
    for (sp_int32 j = 0; j < _topology->bolts(i).outputs_size(); ++j) {
      proto::api::OutputStream* os = _topology->mutable_bolts(i)->mutable_outputs(j);
      std::pair<sp_string, sp_string> p =
          make_pair(os->stream().component_name(), os->stream().id());
      schema_map[p] = os->mutable_schema();
    }
  }

  // Only bolts can consume
  for (sp_int32 i = 0; i < _topology->bolts_size(); ++i) {
    for (sp_int32 j = 0; j < _topology->bolts(i).inputs_size(); ++j) {
      const proto::api::InputStream& is = _topology->bolts(i).inputs(j);
      std::pair<sp_string, sp_string> p = make_pair(is.stream().component_name(), is.stream().id());
      proto::api::StreamSchema* schema = schema_map[p];
      const sp_string& component_name = _topology->bolts(i).comp().name();
      auto iter = _component_to_task_ids.find(component_name);
      CHECK(iter != _component_to_task_ids.end());
      const std::vector<sp_int32>& component_task_ids = iter->second;
      if (stream_consumers_.find(p) == stream_consumers_.end()) {
        stream_consumers_[p] = new StreamConsumers(is, *schema, component_task_ids);
      } else {
        stream_consumers_[p]->NewConsumer(is, *schema, component_task_ids);
      }
    }
  }
}

void StMgr::PopulateXorManagers(
    const proto::api::Topology& _topology, sp_int32 _message_timeout,
    const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids) {
  // Only spouts need xor maintainance
  // TODO(vikasr) Do only for the spouts that we have.
  std::vector<sp_int32> all_spout_tasks;
  for (sp_int32 i = 0; i < _topology.spouts_size(); ++i) {
    for (sp_int32 j = 0; j < _topology.spouts(i).outputs_size(); ++j) {
      const proto::api::OutputStream os = _topology.spouts(i).outputs(j);
      const std::vector<sp_int32>& component_task_ids =
          _component_to_task_ids.find(os.stream().component_name())->second;
      all_spout_tasks.insert(all_spout_tasks.end(), component_task_ids.begin(),
                             component_task_ids.end());
    }
  }
  xor_mgrs_ = new XorManager(eventLoop_, _message_timeout, all_spout_tasks);
}

const proto::system::PhysicalPlan* StMgr::GetPhysicalPlan() const { return pplan_; }

void StMgr::HandleStreamManagerData(const sp_string&,
                                    proto::stmgr::TupleStreamMessage* _message) {
  if (stateful_restorer_ && stateful_restorer_->InProgress()) {
    LOG(INFO) << "Dropping data received from stmgr because we are in Restore";
    dropped_during_restore_metrics_->scope(RESTORE_DROPPED_STMGR_BYTES)
           ->incr_by(_message->set().size());
    __global_protobuf_pool_release__(_message);
    return;
  }
  // We received message from another stream manager
  sp_int32 _task_id = _message->task_id();

  // We have a shortcut for non-acking case
  if (!is_acking_enabled) {
    server_->SendToInstance2(_message);
  } else {
    proto::system::HeronTupleSet2* tuple_set = nullptr;
    tuple_set = __global_protobuf_pool_acquire__(tuple_set);
    tuple_set->ParsePartialFromString(_message->set());
    SendInBound(_task_id, tuple_set);
    __global_protobuf_pool_release__(_message);
  }
}

void StMgr::SendInBound(sp_int32 _task_id, proto::system::HeronTupleSet2* _message) {
  if (_message->has_data()) {
    server_->SendToInstance2(_task_id, _message);
  }
  if (_message->has_control()) {
    // We got a bunch of acks/fails
    ProcessAcksAndFails(_message->src_task_id(), _task_id, _message->control());
    __global_protobuf_pool_release__(_message);
  }
}

void StMgr::ProcessAcksAndFails(sp_int32 _src_task_id, sp_int32 _task_id,
                                const proto::system::HeronControlTupleSet& _control) {
  proto::system::HeronTupleSet2* current_control_tuple_set = nullptr;
  current_control_tuple_set = __global_protobuf_pool_acquire__(current_control_tuple_set);
  current_control_tuple_set->set_src_task_id(_src_task_id);

  // First go over emits. This makes sure that new emits makes
  // a tuples stay alive before we process its acks
  for (sp_int32 i = 0; i < _control.emits_size(); ++i) {
    const proto::system::AckTuple& ack_tuple = _control.emits(i);
    for (sp_int32 j = 0; j < ack_tuple.roots_size(); ++j) {
      CHECK_EQ(_task_id, ack_tuple.roots(j).taskid());
      CHECK(!xor_mgrs_->anchor(_task_id, ack_tuple.roots(j).key(), ack_tuple.ackedtuple()));
    }
  }

  // Then go over acks
  for (sp_int32 i = 0; i < _control.acks_size(); ++i) {
    const proto::system::AckTuple& ack_tuple = _control.acks(i);
    for (sp_int32 j = 0; j < ack_tuple.roots_size(); ++j) {
      CHECK_EQ(_task_id, ack_tuple.roots(j).taskid());
      if (xor_mgrs_->anchor(_task_id, ack_tuple.roots(j).key(), ack_tuple.ackedtuple())) {
        // This tuple tree is all over
        proto::system::AckTuple* a;
        a = current_control_tuple_set->mutable_control()->add_acks();
        proto::system::RootId* r = a->add_roots();
        r->set_key(ack_tuple.roots(j).key());
        r->set_taskid(_task_id);
        a->set_ackedtuple(0);  // this is ignored
        CHECK(xor_mgrs_->remove(_task_id, ack_tuple.roots(j).key()));
      }
    }
  }

  // Now go over the fails
  for (sp_int32 i = 0; i < _control.fails_size(); ++i) {
    const proto::system::AckTuple& fail_tuple = _control.fails(i);
    for (sp_int32 j = 0; j < fail_tuple.roots_size(); ++j) {
      CHECK_EQ(_task_id, fail_tuple.roots(j).taskid());
      if (xor_mgrs_->remove(_task_id, fail_tuple.roots(j).key())) {
        // This tuple tree is failed
        proto::system::AckTuple* f;
        f = current_control_tuple_set->mutable_control()->add_fails();
        proto::system::RootId* r = f->add_roots();
        r->set_key(fail_tuple.roots(j).key());
        r->set_taskid(_task_id);
        f->set_ackedtuple(0);  // this is ignored
      }
    }
  }

  // Check if we need to send this out
  if (current_control_tuple_set->has_control()) {
    server_->SendToInstance2(_task_id, current_control_tuple_set);
  } else {
    __global_protobuf_pool_release__(current_control_tuple_set);
  }
}

// Called when local tasks generate data
void StMgr::HandleInstanceData(const sp_int32 _src_task_id, bool _local_spout,
                               proto::system::HeronTupleSet* _message) {
  if (stateful_restorer_ && stateful_restorer_->InProgress()) {
    LOG(INFO) << "Dropping data received from instance " << _src_task_id
              << " because we are in Restore";
    dropped_during_restore_metrics_->scope(RESTORE_DROPPED_INSTANCE_TUPLES)
           ->incr_by(_message->data().tuples_size());
    return;
  }
  // Note:- Process data before control
  // This is to make sure that anchored emits are sent out
  // before any acks/fails

  if (_message->has_data()) {
    proto::system::HeronDataTupleSet* d = _message->mutable_data();
    std::pair<sp_string, sp_string> stream =
        make_pair(d->stream().component_name(), d->stream().id());
    auto s = stream_consumers_.find(stream);
    if (s != stream_consumers_.end()) {
      StreamConsumers* s_consumer = s->second;
      for (sp_int32 i = 0; i < d->tuples_size(); ++i) {
        proto::system::HeronDataTuple* _tuple = d->mutable_tuples(i);
        // just to make sure that instances do not set any key
        CHECK_EQ(_tuple->key(), 0);
        out_tasks_.clear();
        s_consumer->GetListToSend(*_tuple, out_tasks_);
        // In addition to out_tasks_, the instance might have asked
        // us to send the tuple to some more tasks
        for (sp_int32 j = 0; j < _tuple->dest_task_ids_size(); ++j) {
          out_tasks_.push_back(_tuple->dest_task_ids(j));
        }
        if (out_tasks_.empty()) {
          LOG(ERROR) << "Nobody to send the tuple to";
        }
        // TODO(vikasr) Do a fast path that does not involve copying
        CopyDataOutBound(_src_task_id, _local_spout, d->stream(), _tuple, out_tasks_);
      }
    } else {
      LOG(ERROR) << "Nobody consumes stream " << stream.second << " from component "
                 << stream.first;
    }
  }
  if (_message->has_control()) {
    proto::system::HeronControlTupleSet* c = _message->mutable_control();
    CHECK_EQ(c->emits_size(), 0);
    for (sp_int32 i = 0; i < c->acks_size(); ++i) {
      CopyControlOutBound(_src_task_id, c->acks(i), false);
    }
    for (sp_int32 i = 0; i < c->fails_size(); ++i) {
      CopyControlOutBound(_src_task_id, c->fails(i), true);
    }
  }
}

// Called to drain cached instance data
void StMgr::DrainInstanceData(sp_int32 _task_id, proto::system::HeronTupleSet2* _tuple) {
  const sp_string& dest_stmgr_id = task_id_to_stmgr_[_task_id];
  if (dest_stmgr_id == stmgr_id_) {
    // Our own loopback
    SendInBound(_task_id, _tuple);
  } else {
    bool dropped = !(clientmgr_->SendTupleStreamMessage(_task_id, dest_stmgr_id, *_tuple));
    if (dropped && stateful_restorer_ && !stateful_restorer_->InProgress()) {
      LOG(INFO) << "We dropped some messages because we are not yet connected with stmgr "
                << dest_stmgr_id << " and we are not in restore. Hence sending Reset "
                << "message to TMaster";
      tmaster_client_->SendResetTopologyState("", _task_id, "Dropped Instance Tuples");
      restore_initiated_metrics_->incr();
    }
    __global_protobuf_pool_release__(_tuple);
  }
}

void StMgr::CopyControlOutBound(sp_int32 _src_task_id,
                                const proto::system::AckTuple& _control, bool _is_fail) {
  for (sp_int32 i = 0; i < _control.roots_size(); ++i) {
    proto::system::AckTuple t;
    t.add_roots()->CopyFrom(_control.roots(i));
    t.set_ackedtuple(_control.ackedtuple());
    if (!_is_fail) {
      tuple_cache_->add_ack_tuple(_src_task_id, _control.roots(i).taskid(), t);
    } else {
      tuple_cache_->add_fail_tuple(_src_task_id, _control.roots(i).taskid(), t);
    }
  }
}

void StMgr::CopyDataOutBound(sp_int32 _src_task_id, bool _local_spout,
                             const proto::api::StreamId& _streamid,
                             proto::system::HeronDataTuple* _tuple,
                             const std::vector<sp_int32>& _out_tasks) {
  bool first_iteration = true;
  for (auto& i : _out_tasks) {
    sp_int64 tuple_key = tuple_cache_->add_data_tuple(_src_task_id, i, _streamid, _tuple);
    if (_tuple->roots_size() > 0) {
      // Anchored tuple
      if (_local_spout) {
        // This is a local spout. We need to maintain xors
        CHECK_EQ(_tuple->roots_size(), 1);
        if (first_iteration) {
          xor_mgrs_->create(_src_task_id, _tuple->roots(0).key(), tuple_key);
        } else {
          CHECK(!xor_mgrs_->anchor(_src_task_id, _tuple->roots(0).key(), tuple_key));
        }
      } else {
        // Anchored emits from local bolt
        for (sp_int32 i = 0; i < _tuple->roots_size(); ++i) {
          proto::system::AckTuple t;
          t.add_roots()->CopyFrom(_tuple->roots(i));
          t.set_ackedtuple(tuple_key);
          tuple_cache_->add_emit_tuple(_src_task_id, _tuple->roots(i).taskid(), t);
        }
      }
    }
    first_iteration = false;
  }
}

void StMgr::StartBackPressureOnServer(const sp_string& _other_stmgr_id) {
  // Ask the StMgrServer to stop consuming. The client does
  // not consume anything
  server_->StartBackPressureClientCb(_other_stmgr_id);
}

void StMgr::StopBackPressureOnServer(const sp_string& _other_stmgr_id) {
  // Call the StMgrServers removeBackPressure method
  server_->StopBackPressureClientCb(_other_stmgr_id);
}

void StMgr::SendStartBackPressureToOtherStMgrs() {
  clientmgr_->SendStartBackPressureToOtherStMgrs();
}

void StMgr::SendStopBackPressureToOtherStMgrs() { clientmgr_->SendStopBackPressureToOtherStMgrs(); }

// Do any actions if a stmgr client connection dies
void StMgr::HandleDeadStMgrConnection(const sp_string& _stmgr_id) {
  // If we are stateful topology, we need to send a resetTopology message
  // in case we are not in 2pc
  if (stateful_restorer_) {
    if (!stateful_restorer_->InProgress() && tmaster_client_) {
      LOG(INFO) << "We lost connection with stmgr " << _stmgr_id
                << " and hence sending ResetTopology message to tmaster";
      tmaster_client_->SendResetTopologyState(_stmgr_id, -1, "Dead Stmgr");
      restore_initiated_metrics_->incr();
    } else {
      // We are in restore
      stateful_restorer_->HandleDeadStMgrConnection();
    }
  }
}

void StMgr::HandleAllStMgrClientsRegistered() {
  // If we are stateful topology, we might want to continue our restore process
  if (stateful_restorer_) {
    stateful_restorer_->HandleAllStMgrClientsConnected();
  }
}

void StMgr::HandleAllInstancesConnected() {
  // StmgrServer told us that all instances are connected to us
  if (stateful_restorer_) {
    if (stateful_restorer_->InProgress()) {
      // We are in the middle of a restore
      stateful_restorer_->HandleAllInstancesConnected();
    } else if (tmaster_client_ && tmaster_client_->IsConnected()) {
      LOG(INFO) << "We are already connected to tmaster(which means we are not in"
                << " initial startup), and we are not in the middle of restore."
                << " This means that while running normally, some instances"
                << " got reconnected to us and thus we might have lost some tuples in middle"
                << " We must reset the topology";
      tmaster_client_->SendResetTopologyState("", -1, "All Instances connected");
      restore_initiated_metrics_->incr();
    } else {
      // This is the first time we came up when we haven't even connected to tmaster
      // Now that all instances are connected to us, we should connect to tmaster
      StartTMasterClient();
    }
  } else {
    // Now we can connect to the tmaster
    StartTMasterClient();
  }
}

void StMgr::HandleDeadInstance(sp_int32 _task_id) {
  if (stateful_restorer_) {
    if (stateful_restorer_->InProgress()) {
      stateful_restorer_->HandleDeadInstanceConnection(_task_id);
    } else {
      LOG(INFO) << "An instance " << _task_id << " died while we are not "
                << "in restore. Sending ResetMessage to tmaster";
      tmaster_client_->SendResetTopologyState("", _task_id, "Dead Instance");
      restore_initiated_metrics_->incr();
    }
  }
}

// Invoked by the CheckpointMgr Client when it gets registered to
// the ckptmgr.
void StMgr::HandleCkptMgrRegistration() {
  if (stateful_restorer_) {
    stateful_restorer_->HandleCkptMgrRestart();
  }
}

void StMgr::InitiateStatefulCheckpoint(sp_string _checkpoint_id) {
  // Initiate the process of stateful checkpointing by sending a request
  // to checkpoint to all local spouts
  server_->InitiateStatefulCheckpoint(_checkpoint_id);
}

// We just recieved a InstanceStateCheckpoint message from one of our instances
// We need to propagate it to all downstream tasks
// We also need to send the checkpoint to ckptmgr
void StMgr::HandleStoreInstanceStateCheckpoint(
            const proto::ckptmgr::InstanceStateCheckpoint& _message,
            const proto::system::Instance& _instance) {
  CHECK(stateful_restorer_);
  int32_t task_id = _instance.info().task_id();
  LOG(INFO) << "Got a checkpoint state message from " << task_id
            << " for checkpoint " << _message.checkpoint_id();
  if (stateful_restorer_->InProgress()) {
    LOG(INFO) << "Ignoring the message because we are in progress";
    return;
  }
  std::unordered_set<sp_int32> downstream_receivers =
                    neighbour_calculator_->get_downstreamers(task_id);
  for (auto downstream_receiver : downstream_receivers) {
    LOG(INFO) << "Adding a DownstreamCheckpointMessage triplet "
              << _message.checkpoint_id() << " "
              << task_id << " " << downstream_receiver;
    proto::ckptmgr::DownstreamStatefulCheckpoint* message =
      new proto::ckptmgr::DownstreamStatefulCheckpoint();
    message->set_origin_task_id(task_id);
    message->set_destination_task_id(downstream_receiver);
    message->set_checkpoint_id(_message.checkpoint_id());
    tuple_cache_->add_checkpoint_tuple(downstream_receiver, message);
  }

  // save the checkpoint
  proto::ckptmgr::SaveInstanceStateRequest* message =
         new proto::ckptmgr::SaveInstanceStateRequest();
  message->mutable_instance()->CopyFrom(_instance);
  message->mutable_checkpoint()->CopyFrom(_message);
  ckptmgr_client_->SaveInstanceState(message);
}

// Invoked by CheckpointMgr Client when it finds out that the ckptmgr
// saved the state of an instance
void StMgr::HandleSavedInstanceState(const proto::system::Instance& _instance,
                                     const std::string& _checkpoint_id) {
  LOG(INFO) << "Got notification from ckptmgr that we saved instance state for task "
            << _instance.info().task_id() << " for checkpoint "
            << _checkpoint_id;
  tmaster_client_->SavedInstanceState(_instance, _checkpoint_id);
}

// Invoked by CheckpointMgr Client when it retreives the state of an instance
void StMgr::HandleGetInstanceState(proto::system::StatusCode _status, sp_int32 _task_id,
                                   sp_string _checkpoint_id,
                                   const proto::ckptmgr::InstanceStateCheckpoint& _msg) {
  if (stateful_restorer_) {
    stateful_restorer_->HandleCheckpointState(_status, _task_id, _checkpoint_id, _msg);
  }
}

// Send checkpoint message to this task_id
void StMgr::DrainDownstreamCheckpoint(sp_int32 _task_id,
                                      proto::ckptmgr::DownstreamStatefulCheckpoint* _message) {
  sp_string stmgr = task_id_to_stmgr_[_task_id];
  if (stmgr == stmgr_id_) {
    HandleDownStreamStatefulCheckpoint(*_message);
    delete _message;
  } else {
    clientmgr_->SendDownstreamStatefulCheckpoint(stmgr, _message);
  }
}

void StMgr::HandleDownStreamStatefulCheckpoint(
            const proto::ckptmgr::DownstreamStatefulCheckpoint& _message) {
  server_->HandleCheckpointMarker(_message.origin_task_id(),
                                  _message.destination_task_id(),
                                  _message.checkpoint_id());
}

// Called by TmasterClient when it receives directive from tmaster
// to restore the topology to _checkpoint_id checkpoint
void StMgr::RestoreTopologyState(sp_string _checkpoint_id, sp_int64 _restore_txid) {
  LOG(INFO) << "Got a Restore Topology State message from Tmaster for checkpoint "
            << _checkpoint_id << " and txid " << _restore_txid;
  CHECK(stateful_restorer_);

  // Start the restore process
  std::unordered_set<sp_int32> local_taskids;
  config::PhysicalPlanHelper::GetTasks(*pplan_, stmgr_id_, local_taskids),
  stateful_restorer_->StartRestore(_checkpoint_id, _restore_txid, local_taskids, pplan_);
}

// Called by TmasterClient when it receives directive from tmaster
// to start processing after having previously recovered the state at _checkpoint_id
void StMgr::StartStatefulProcessing(sp_string _checkpoint_id) {
  LOG(INFO) << "Received StartProcessing message from tmaster for "
            << _checkpoint_id;
  CHECK(stateful_restorer_);
  if (stateful_restorer_->InProgress()) {
    LOG(FATAL) << "StartProcessing received from Tmaster for "
               << _checkpoint_id << " when we are still in Restore";
  }
  server_->SendStartInstanceStatefulProcessing(_checkpoint_id);
}

void StMgr::HandleRestoreInstanceStateResponse(sp_int32 _task_id,
                                               const proto::system::Status& _status,
                                               const std::string& _checkpoint_id) {
  // If we are stateful topology, we might want to see how the restore went
  // and if it was successful and all other local instances have recovered
  // send back a success response to tmaster saying that we have recovered
  CHECK(stateful_restorer_);
  stateful_restorer_->HandleInstanceRestoredState(_task_id, _status.status(), _checkpoint_id);
}

// Called after we have recovered our state(either successfully or unsuccessfully)
// We need to let our tmaster know
void StMgr::HandleStatefulRestoreDone(proto::system::StatusCode _status,
                                      std::string _checkpoint_id, sp_int64 _restore_txid) {
  tmaster_client_->SendRestoreTopologyStateResponse(_status, _checkpoint_id, _restore_txid);
}
}  // namespace stmgr
}  // namespace heron
