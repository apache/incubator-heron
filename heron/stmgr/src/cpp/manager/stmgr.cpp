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
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"
#include "config/helper.h"
#include "statemgr/heron-statemgr.h"
#include "metrics/metrics.h"
#include "metrics/metrics-mgr-st.h"
#include "util/xor-manager.h"
#include "manager/tmaster-client.h"
#include "util/tuple-cache.h"

namespace heron {
namespace stmgr {

// Stats for the process
const sp_string METRIC_CPU_USER = "__cpu_user_usec";
const sp_string METRIC_CPU_SYSTEM = "__cpu_system_usec";
const sp_string METRIC_UPTIME = "__uptime_sec";
const sp_string METRIC_MEM_USED = "__mem_used_bytes";
const sp_int64 PROCESS_METRICS_FREQUENCY = 10 * 1000 * 1000;
const sp_int64 TMASTER_RETRY_FREQUENCY = 10 * 1000 * 1000;  // in micro seconds

StMgr::StMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
             const sp_string& _topology_id, proto::api::Topology* _hydrated_topology,
             const sp_string& _stmgr_id, const std::vector<sp_string>& _instances,
             const sp_string& _zkhostport, const sp_string& _zkroot, sp_int32 _metricsmgr_port,
             sp_int32 _shell_port)
    : pplan_(NULL),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
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
      shell_port_(_shell_port) {}

void StMgr::Init() {
  LOG(INFO) << "Init Stmgr" << std::endl;
  sp_int32 metrics_export_interval_sec =
      config::HeronInternalsConfigReader::Instance()->GetHeronMetricsExportIntervalSec();

  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(zkhostport_, zkroot_, eventLoop_, false);
  metrics_manager_client_ = new heron::common::MetricsMgrSt(
      IpUtils::getHostName(), stmgr_port_, metricsmgr_port_, "__stmgr__", stmgr_id_,
      metrics_export_interval_sec, eventLoop_);
  stmgr_process_metrics_ = new heron::common::MultiAssignableMetric();
  metrics_manager_client_->register_metric("__process", stmgr_process_metrics_);
  state_mgr_->SetTMasterLocationWatch(topology_name_, [this]() { this->FetchTMasterLocation(); });

  FetchTMasterLocation();

  CHECK_GT(
      eventLoop_->registerTimer(
          [this](EventLoop::Status status) { this->CheckTMasterLocation(status); }, false,
          config::HeronInternalsConfigReader::Instance()->GetCheckTMasterLocationIntervalSec() *
              1000 * 1000),
      0);  // fire only once

  // Create and start StmgrServer
  StartStmgrServer();
  // Create and Register Tuple cache
  CreateTupleCache();

  // Check for log pruning every 5 minutes
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::PruneLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingPruneIntervalSec() *
                   1000 * 1000),
           0);

  // Check for log flushing every 10 seconds
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::FlushLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingFlushIntervalSec() *
                   1000 * 1000),
           0);

  // Update Process related metrics every 10 seconds
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateProcessMetrics(status);
  }, true, PROCESS_METRICS_FREQUENCY), 0);

  is_acking_enabled =
    heron::config::TopologyConfigHelper::IsAckingEnabled(*hydrated_topology_);

  tuple_set_from_other_stmgr_ = new proto::system::HeronTupleSet2();
}

StMgr::~StMgr() {
  metrics_manager_client_->unregister_metric("__process");
  delete stmgr_process_metrics_;
  delete tuple_cache_;
  delete state_mgr_;
  delete pplan_;
  delete server_;
  delete clientmgr_;
  delete tmaster_client_;
  CleanupStreamConsumers();
  CleanupXorManagers();
  delete hydrated_topology_;
  delete metrics_manager_client_;

  delete tuple_set_from_other_stmgr_;
}

bool StMgr::DidAnnounceBackPressure() { return server_->DidAnnounceBackPressure(); }

void StMgr::CheckTMasterLocation(EventLoop::Status) {
  if (!tmaster_client_) {
    LOG(FATAL) << "Could not fetch the TMaster location in time. Exiting. " << std::endl;
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
      ->SetValue((usage.ru_utime.tv_sec * 1000 * 1000) + usage.ru_utime.tv_usec);
  stmgr_process_metrics_->scope(METRIC_CPU_SYSTEM)
      ->SetValue((usage.ru_stime.tv_sec * 1000 * 1000) + usage.ru_stime.tv_usec);
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

void StMgr::StartStmgrServer() {
  CHECK(!server_);
  LOG(INFO) << "Creating StmgrServer" << std::endl;
  NetworkOptions sops;
  sops.set_host(IpUtils::getHostName());
  sops.set_port(stmgr_port_);
  sops.set_socket_family(PF_INET);
  sops.set_max_packet_size(std::numeric_limits<sp_uint32>::max() - 1);
  server_ = new StMgrServer(eventLoop_, sops, topology_name_, topology_id_, stmgr_id_, instances_,
                            this, metrics_manager_client_);

  // start the server
  CHECK_EQ(server_->Start(), 0);
}

void StMgr::CreateTMasterClient(proto::tmaster::TMasterLocation* tmasterLocation) {
  CHECK(!tmaster_client_);
  LOG(INFO) << "Creating Tmaster Client at " << tmasterLocation->host() << ":"
            << tmasterLocation->master_port() << std::endl;
  NetworkOptions master_options;
  master_options.set_host(tmasterLocation->host());
  master_options.set_port(tmasterLocation->master_port());
  master_options.set_socket_family(PF_INET);
  master_options.set_max_packet_size(std::numeric_limits<sp_uint32>::max() - 1);
  auto pplan_watch = [this](proto::system::PhysicalPlan* pplan) { this->NewPhysicalPlan(pplan); };

  tmaster_client_ = new TMasterClient(eventLoop_, master_options, stmgr_id_, stmgr_port_,
                                      shell_port_, std::move(pplan_watch));
}

void StMgr::CreateTupleCache() {
  CHECK(!tuple_cache_);
  LOG(INFO) << "Creating tuple cache " << std::endl;
  sp_uint32 drain_threshold_bytes_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrCacheDrainSizeMb() * 1024 *
      1024;
  tuple_cache_ = new TupleCache(eventLoop_, drain_threshold_bytes_);

  tuple_cache_->RegisterDrainer(&StMgr::DrainInstanceData, this);
}

void StMgr::HandleNewTmaster(proto::tmaster::TMasterLocation* newTmasterLocation) {
  // Lets delete the existing tmaster if we have one.
  if (tmaster_client_) {
    LOG(INFO) << "Destroying existing tmasterClient" << std::endl;
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

  // TODO(vikasr): See if the the creation of StMgrClientMgr can be done
  // in the constructor rather than here.
  if (!clientmgr_) {
    clientmgr_ = new StMgrClientMgr(eventLoop_, topology_name_, topology_id_, stmgr_id_, this,
                                    metrics_manager_client_);
  }
}

void StMgr::BroadcastTmasterLocation(proto::tmaster::TMasterLocation* tmasterLocation) {
  // Notify metrics manager of the tmaster location changes
  // TODO(vikasr): What if the refresh fails?
  metrics_manager_client_->RefreshTMasterLocation(*tmasterLocation);
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
                 << newTmasterLocation->topology_id() << std::endl;
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
                  << "Nothing to do here... " << std::endl;
        isNewTmaster = false;
      } else {
        LOG(INFO) << "New tmaster location different from the current one."
                  << " Current one at " << currentTmasterHostPort << " and New one at "
                  << newTmasterHostPort << std::endl;
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

// Start the tmaster client
void StMgr::StartTMasterClient() {
  if (!tmaster_client_) {
    LOG(INFO) << "We haven't received tmaster location yet"
              << ", so tmaster_client_ hasn't been created"
              << "Once we get the location, it will be started" << std::endl;
    // Nothing else to do here
  } else {
    std::vector<proto::system::Instance*> all_instance_info;
    server_->GetInstanceInfo(all_instance_info);
    tmaster_client_->SetInstanceInfo(all_instance_info);
    if (!tmaster_client_->IsConnected()) {
      LOG(INFO) << "Connecting to the TMaster as all the instances have connected to us"
                << std::endl;
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
    LOG(FATAL) << "We have no role in this topology!!" << std::endl;
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
  clientmgr_->NewPhysicalPlan(pplan_);
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
  LOG(FATAL) << "topology.message.timeout.secs does not exist" << std::endl;
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
                                    const proto::stmgr::TupleStreamMessage2& _message) {
  // We received message from another stream manager
  sp_int32 _task_id = _message.task_id();

  // We have a shortcut for non-acking case
  if (!is_acking_enabled) {
    server_->SendToInstance2(_task_id, _message.set().size(),
                             heron_tuple_set_2_, _message.set().c_str());
  } else {
    tuple_set_from_other_stmgr_->ParsePartialFromString(_message.set());

    SendInBound(_task_id, tuple_set_from_other_stmgr_);
  }
}

void StMgr::SendInBound(sp_int32 _task_id, proto::system::HeronTupleSet2* _message) {
  if (_message->has_data()) {
    server_->SendToInstance2(_task_id, *_message);
  }
  if (_message->has_control()) {
    // We got a bunch of acks/fails
    ProcessAcksAndFails(_task_id, _message->control());
  }
}

void StMgr::ProcessAcksAndFails(sp_int32 _task_id,
                                const proto::system::HeronControlTupleSet& _control) {
  current_control_tuple_set_.Clear();

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
        a = current_control_tuple_set_.mutable_control()->add_acks();
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
        f = current_control_tuple_set_.mutable_control()->add_fails();
        proto::system::RootId* r = f->add_roots();
        r->set_key(fail_tuple.roots(j).key());
        r->set_taskid(_task_id);
        f->set_ackedtuple(0);  // this is ignored
      }
    }
  }

  // Check if we need to send this out
  if (current_control_tuple_set_.has_control()) {
    server_->SendToInstance2(_task_id, current_control_tuple_set_);
  }
}

// Called when local tasks generate data
void StMgr::HandleInstanceData(const sp_int32 _src_task_id, bool _local_spout,
                               proto::system::HeronTupleSet* _message) {
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
      CopyControlOutBound(c->acks(i), false);
    }
    for (sp_int32 i = 0; i < c->fails_size(); ++i) {
      CopyControlOutBound(c->fails(i), true);
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
    clientmgr_->SendTupleStreamMessage(_task_id, dest_stmgr_id, *_tuple);
  }

  tuple_cache_->release(_task_id, _tuple);
}

void StMgr::CopyControlOutBound(const proto::system::AckTuple& _control, bool _is_fail) {
  for (sp_int32 i = 0; i < _control.roots_size(); ++i) {
    proto::system::AckTuple t;
    t.add_roots()->CopyFrom(_control.roots(i));
    t.set_ackedtuple(_control.ackedtuple());
    if (!_is_fail) {
      tuple_cache_->add_ack_tuple(_control.roots(i).taskid(), t);
    } else {
      tuple_cache_->add_fail_tuple(_control.roots(i).taskid(), t);
    }
  }
}

void StMgr::CopyDataOutBound(sp_int32 _src_task_id, bool _local_spout,
                             const proto::api::StreamId& _streamid,
                             proto::system::HeronDataTuple* _tuple,
                             const std::vector<sp_int32>& _out_tasks) {
  bool first_iteration = true;
  for (auto& i : _out_tasks) {
    sp_int64 tuple_key = tuple_cache_->add_data_tuple(i, _streamid, _tuple);
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
          tuple_cache_->add_emit_tuple(_tuple->roots(i).taskid(), t);
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

}  // namespace stmgr
}  // namespace heron
