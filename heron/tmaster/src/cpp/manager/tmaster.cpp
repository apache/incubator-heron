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

#include "manager/tmaster.h"
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
#include "manager/tmasterserver.h"
#include "manager/stmgrstate.h"
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
#include "metrics/tmaster-metrics.h"

namespace heron {
namespace tmaster {

// Stats for the process
const sp_string METRIC_CPU_USER = "__cpu_user_usec";
const sp_string METRIC_CPU_SYSTEM = "__cpu_system_usec";
const sp_string METRIC_UPTIME = "__uptime_sec";
const sp_string METRIC_MEM_USED = "__mem_used_bytes";
const sp_int64 PROCESS_METRICS_FREQUENCY = 60 * 1000 * 1000;
const sp_string METRIC_PREFIX = "__process";

TMaster::TMaster(const std::string& _zk_hostport, const std::string& _topology_name,
                 const std::string& _topology_id, const std::string& _topdir,
                 const std::vector<std::string>& _stmgrs, sp_int32 _controller_port,
                 sp_int32 _master_port, sp_int32 _stats_port, sp_int32 metricsMgrPort,
                 const std::string& _metrics_sinks_yaml, const std::string& _myhost_name,
                 EventLoop* eventLoop) {
  start_time_ = std::chrono::high_resolution_clock::now();
  zk_hostport_ = _zk_hostport;
  topdir_ = _topdir;
  controller_ = NULL;
  controller_port_ = _controller_port;
  master_ = NULL;
  master_port_ = _master_port;
  stats_ = NULL;
  stats_port_ = _stats_port;
  myhost_name_ = _myhost_name;
  eventLoop_ = eventLoop;
  metrics_collector_ =
      new TMetricsCollector(config::HeronInternalsConfigReader::Instance()
                                    ->GetHeronTmasterMetricsCollectorMaximumIntervalMin() *
                                60,
                            eventLoop_, _metrics_sinks_yaml);

  mMetricsMgrPort = metricsMgrPort;

  sp_int32 metricsExportIntervalSec =
      config::HeronInternalsConfigReader::Instance()->GetHeronMetricsExportIntervalSec();

  mMetricsMgrClient = new heron::common::MetricsMgrSt(
      IpUtils::getHostName(), master_port_, mMetricsMgrPort, "__tmaster__",
      "0",  // MM expects task_id, so just giving 0 for tmaster.
      metricsExportIntervalSec, eventLoop_);

  tmasterProcessMetrics = new heron::common::MultiAssignableMetric();
  mMetricsMgrClient->register_metric(METRIC_PREFIX, tmasterProcessMetrics);

  // We will keep the list of stmgrs with us.
  // In case a assignment already exists, we will throw this
  // list out and re-init with whats in assignment.
  for (sp_uint32 i = 0; i < _stmgrs.size(); ++i) {
    absent_stmgrs_.insert(_stmgrs[i]);
  }

  current_pplan_ = NULL;

  // The topology as first submitted by the user
  // It shall only be used to construct the physical plan when TMaster first time starts
  // Any runtime changes shall be made to current_pplan_->topology
  topology_ = NULL;
  state_mgr_ = heron::common::HeronStateMgr::MakeStateMgr(zk_hostport_, topdir_, eventLoop_);

  assignment_in_progress_ = false;
  do_reassign_ = false;

  master_establish_attempts_ = 0;
  tmaster_location_ = new proto::tmaster::TMasterLocation();
  tmaster_location_->set_topology_name(_topology_name);
  tmaster_location_->set_topology_id(_topology_id);
  tmaster_location_->set_host(myhost_name_);
  tmaster_location_->set_controller_port(controller_port_);
  tmaster_location_->set_master_port(master_port_);
  tmaster_location_->set_stats_port(stats_port_);
  DCHECK(tmaster_location_->IsInitialized());
  EstablishTMaster(EventLoop::TIMEOUT_EVENT);

  // Send tmaster location to metrics mgr
  mMetricsMgrClient->RefreshTMasterLocation(*tmaster_location_);

  // Check for log pruning every 5 minutes
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::PruneLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingPruneIntervalSec() *
                   1000 * 1000),
           0);

  // Flush logs every 10 seconds
  CHECK_GT(eventLoop_->registerTimer(
               [](EventLoop::Status) { ::heron::common::FlushLogs(); }, true,
               config::HeronInternalsConfigReader::Instance()->GetHeronLoggingFlushIntervalSec() *
                   1000 * 1000),
           0);

  // Update Process related metrics every 60 seconds
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateProcessMetrics(status);
  }, true, PROCESS_METRICS_FREQUENCY), 0);
}

void TMaster::EstablishTMaster(EventLoop::Status) {
  auto cb = [this](proto::system::StatusCode code) { this->SetTMasterLocationDone(code); };

  state_mgr_->SetTMasterLocation(*tmaster_location_, std::move(cb));
  master_establish_attempts_++;
}

TMaster::~TMaster() {
  delete topology_;
  delete current_pplan_;
  delete state_mgr_;
  delete controller_;
  if (master_) {
    master_->Stop();
  }
  delete master_;
  delete stats_;
  delete tmaster_location_;
  for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
    delete iter->second;
  }
  stmgrs_.clear();
  delete metrics_collector_;

  mMetricsMgrClient->unregister_metric(METRIC_PREFIX);
  delete mMetricsMgrClient;
  delete tmasterProcessMetrics;
}

void TMaster::UpdateProcessMetrics(EventLoop::Status) {
  // Uptime
  auto delta = std::chrono::duration_cast<std::chrono::seconds>(
                   std::chrono::high_resolution_clock::now() - start_time_)
                   .count();
  tmasterProcessMetrics->scope(METRIC_UPTIME)->SetValue(delta);

  // CPU
  struct rusage usage;
  ProcessUtils::getResourceUsage(&usage);
  tmasterProcessMetrics->scope(METRIC_CPU_USER)
      ->SetValue((usage.ru_utime.tv_sec * 1000 * 1000) + usage.ru_utime.tv_usec);
  tmasterProcessMetrics->scope(METRIC_CPU_SYSTEM)
      ->SetValue((usage.ru_stime.tv_sec * 1000 * 1000) + usage.ru_stime.tv_usec);
  // Memory
  size_t totalmemory = ProcessUtils::getTotalMemoryUsed();
  tmasterProcessMetrics->scope(METRIC_MEM_USED)->SetValue(totalmemory);
}

void TMaster::SetTMasterLocationDone(proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    if (_code == proto::system::TMASTERLOCATION_ALREADY_EXISTS &&
        master_establish_attempts_ <
            config::HeronInternalsConfigReader::Instance()->GetHeronTmasterEstablishRetryTimes()) {
      LOG(INFO) << "Topology Master node already exists. Maybe its "
                << "because of our restart. Will try again" << std::endl;
      // Attempt again
      auto cb = [this](EventLoop::Status status) { this->EstablishTMaster(status); };
      eventLoop_->registerTimer(std::move(cb), false,
                                config::HeronInternalsConfigReader::Instance()
                                        ->GetHeronTmasterEstablishRetryIntervalSec() *
                                    1000000);
      return;
    }
    // There was an error setting our location
    LOG(ERROR) << "For topology " << tmaster_location_->topology_name()
               << " Error setting ourselves as TMaster. Errorcode is " << _code << std::endl;
    ::exit(1);
  }

  master_establish_attempts_ = 0;

  // We are now the master
  LOG(INFO) << "Successfully set ourselves as master\n";
  // Lets now read the topology
  auto cb = [this](proto::system::StatusCode code) { this->GetTopologyDone(code); };

  topology_ = new proto::api::Topology();
  state_mgr_->GetTopology(tmaster_location_->topology_name(), topology_, std::move(cb));
}

void TMaster::GetTopologyDone(proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    // Without Topology we can't do much
    LOG(ERROR) << "For topology " << tmaster_location_->topology_name()
               << " Error getting topology Errsettingorcode is " << _code << std::endl;
    ::exit(1);
  }
  // Ok things are fine. topology_ contains the result
  // Just make sure it makes sense.
  CHECK_NOTNULL(topology_);

  if (!ValidateTopology(*topology_)) {
    LOG(ERROR) << "Topology invalid" << std::endl;
    ::exit(1);
  }
  LOG(INFO) << "Topology Read and Validated\n";
  // Now see if there is already a pplan
  proto::system::PhysicalPlan* pplan = new proto::system::PhysicalPlan();
  auto cb = [pplan, this](proto::system::StatusCode code) {
    this->GetPhysicalPlanDone(pplan, code);
  };

  state_mgr_->GetPhysicalPlan(tmaster_location_->topology_name(), pplan, std::move(cb));
}

void TMaster::GetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan,
                                  proto::system::StatusCode _code) {
  // Assignment need not exist. First check if some other
  // error occured.
  if (_code != proto::system::OK && _code != proto::system::PATH_DOES_NOT_EXIST) {
    // Something bad happened. Bail out!
    // TODO(kramasamy): This is not as bad as it seems. Maybe we can delete this assignment
    // and have a new assignment instead.
    LOG(ERROR) << "For topology " << tmaster_location_->topology_name()
               << " Error getting assignment Errsettingorcode is " << _code << std::endl;
    ::exit(1);
  }

  if (_code == proto::system::PATH_DOES_NOT_EXIST) {
    LOG(ERROR) << "There was no existing assignment\n";
    // We never did assignment in the first place
    delete _pplan;
  } else {
    LOG(INFO) << "There was an existing assignment\n";
    CHECK_EQ(_code, proto::system::OK);
    current_pplan_ = _pplan;
    absent_stmgrs_.clear();
    for (sp_int32 i = 0; i < current_pplan_->stmgrs_size(); ++i) {
      absent_stmgrs_.insert(current_pplan_->stmgrs(i).id());
    }
  }

  // Now that we have our state all setup, its time to start accepting requests
  // Port for the stmgrs to connect to
  NetworkOptions master_options;
  master_options.set_host(myhost_name_);
  master_options.set_port(master_port_);
  master_options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                         ->GetHeronTmasterNetworkMasterOptionsMaximumPacketMb() *
                                     1024 * 1024);
  master_options.set_socket_family(PF_INET);
  master_ = new TMasterServer(eventLoop_, master_options, metrics_collector_, this);

  sp_int32 retval = master_->Start();
  if (retval != SP_OK) {
    LOG(FATAL) << "Failed to start TMaster Master Server with rcode: " << retval;
  }

  // Port for the scheduler to connect to
  NetworkOptions controller_options;
  controller_options.set_host(myhost_name_);
  controller_options.set_port(controller_port_);
  controller_options.set_max_packet_size(
      config::HeronInternalsConfigReader::Instance()
          ->GetHeronTmasterNetworkControllerOptionsMaximumPacketMb() *
      1024 * 1024);
  controller_options.set_socket_family(PF_INET);
  controller_ = new TController(eventLoop_, controller_options, this);

  retval = controller_->Start();
  if (retval != SP_OK) {
    LOG(FATAL) << "Failed to start TMaster Controller Server with rcode: " << retval;
  }

  // Http port for stat queries
  NetworkOptions stats_options;
  if (config::HeronInternalsConfigReader::Instance()
          ->GetHeronTmasterMetricsNetworkBindAllInterfaces()) {
    stats_options.set_host("0.0.0.0");
  } else {
    stats_options.set_host(myhost_name_);
  }
  stats_options.set_port(stats_port_);
  stats_options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                        ->GetHeronTmasterNetworkStatsOptionsMaximumPacketMb() *
                                    1024 * 1024);
  stats_options.set_socket_family(PF_INET);
  stats_ = new StatsInterface(eventLoop_, stats_options, metrics_collector_, this);
}

void TMaster::ActivateTopology(VCallback<proto::system::StatusCode> cb) {
  CHECK_EQ(current_pplan_->topology().state(), proto::api::PAUSED);
  DCHECK(current_pplan_->topology().IsInitialized());

  // Set the status
  proto::system::PhysicalPlan* new_pplan = new proto::system::PhysicalPlan();
  new_pplan->CopyFrom(*current_pplan_);
  new_pplan->mutable_topology()->set_state(proto::api::RUNNING);

  auto callback = [new_pplan, this, cb](proto::system::StatusCode code) {
    cb(code);
    this->SetPhysicalPlanDone(new_pplan, code);
  };

  state_mgr_->SetPhysicalPlan(*new_pplan, std::move(callback));
}

void TMaster::DeActivateTopology(VCallback<proto::system::StatusCode> cb) {
  CHECK_EQ(current_pplan_->topology().state(), proto::api::RUNNING);
  DCHECK(current_pplan_->topology().IsInitialized());

  // Set the status
  proto::system::PhysicalPlan* new_pplan = new proto::system::PhysicalPlan();
  new_pplan->CopyFrom(*current_pplan_);
  new_pplan->mutable_topology()->set_state(proto::api::PAUSED);

  auto callback = [new_pplan, this, cb](proto::system::StatusCode code) {
    cb(code);
    this->SetPhysicalPlanDone(new_pplan, code);
  };

  state_mgr_->SetPhysicalPlan(*new_pplan, std::move(callback));
}

proto::system::Status* TMaster::RegisterStMgr(
    const proto::system::StMgr& _stmgr, const std::vector<proto::system::Instance*>& _instances,
    Connection* _conn, proto::system::PhysicalPlan*& _pplan) {
  const std::string& stmgr_id = _stmgr.id();
  LOG(INFO) << "Got a register stmgr request from " << stmgr_id << std::endl;

  // First check if there are any other stream manager present with the same id
  if (stmgrs_.find(stmgr_id) != stmgrs_.end()) {
    // Some other dude is already present with us.
    // First check to see if that other guy has timed out
    if (!stmgrs_[stmgr_id]->TimedOut()) {
      // we reject the new guy
      LOG(ERROR) << "Another stmgr exists with the same id and it hasn't timed out" << std::endl;
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
      LOG(ERROR) << "Another stmgr exists with the same id but it has timed out" << std::endl;
      stmgrs_[stmgr_id]->UpdateWithNewStMgr(_stmgr, _instances, _conn);
      connection_to_stmgr_id_[_conn] = stmgr_id;
    }
  } else if (absent_stmgrs_.find(stmgr_id) == absent_stmgrs_.end()) {
    // Check to see if we were expecting this guy
    LOG(ERROR) << "We were not expecting this stmgr" << std::endl;
    proto::system::Status* status = new proto::system::Status();
    status->set_status(proto::system::INVALID_STMGR);
    status->set_message("Invalid StreamManager");
    return status;
  } else {
    // This guy was indeed expected
    stmgrs_[stmgr_id] = new StMgrState(_conn, _stmgr, _instances, master_);
    connection_to_stmgr_id_[_conn] = stmgr_id;
    absent_stmgrs_.erase(stmgr_id);
  }

  if (absent_stmgrs_.empty()) {
    if (assignment_in_progress_) {
      do_reassign_ = true;
    } else {
      assignment_in_progress_ = true;
      LOG(INFO) << "All stmgrs have connected with us" << std::endl;
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

void TMaster::DoPhysicalPlan(EventLoop::Status) {
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
  proto::system::PhysicalPlan* pplan = MakePhysicalPlan();
  CHECK_NOTNULL(pplan);
  DCHECK(pplan->IsInitialized());

  if (!ValidateStMgrsWithTopology(pplan->topology())) {
    // TODO(kramasamy): Do Something better here
    LOG(ERROR) << "Topology and StMgr mismatch... Dying\n";
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

void TMaster::SetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan,
                                  proto::system::StatusCode _code) {
  if (_code != proto::system::OK) {
    LOG(ERROR) << "Error writing assignment to statemgr. Errocode is " << _code << std::endl;
    ::exit(1);
  }

  LOG(INFO) << "Successfully wrote new assignment to state" << std::endl;

  if (do_reassign_) {
    // Some other mapping change happened
    delete _pplan;
    assignment_in_progress_ = true;
    LOG(INFO) << "Doing assignment since physical assignment might have changed" << std::endl;
    auto cb = [this](EventLoop::Status status) { this->DoPhysicalPlan(status); };
    CHECK_GE(eventLoop_->registerTimer(std::move(cb), false, 0), 0);
  } else {
    delete current_pplan_;
    current_pplan_ = _pplan;
    assignment_in_progress_ = false;
    // We need to pass that on to all streammanagers
    DistributePhysicalPlan();
  }
}

bool TMaster::DistributePhysicalPlan() {
  if (current_pplan_) {
    // First valid the physical plan to distribute
    LOG(INFO) << "To distribute new pplan:" << std::endl;
    config::PhysicalPlanHelper::LogPhysicalPlan(*current_pplan_);

    // Distribute physical plan to all active stmgrs
    StMgrMapIter iter;
    for (iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
      iter->second->NewPhysicalPlan(*current_pplan_);
    }

    return true;
  }

  LOG(ERROR) << "No valid assignment yet" << std::endl;
  return false;
}

proto::system::PhysicalPlan* TMaster::MakePhysicalPlan() {
  // TODO(kramasamy): At some point, we need to talk to our scheduler
  // and do this scheduling
  if (current_pplan_) {
    // There is already an existing assignment. However stmgrs might have
    // died and come up on different machines. This means that
    // we need to just adjust the stmgrs mapping
    // First lets verify that our original pplan and instances
    // all match up
    CHECK(ValidateStMgrsWithPhysicalPlan(*current_pplan_));
    proto::system::PhysicalPlan* new_pplan = new proto::system::PhysicalPlan();
    new_pplan->mutable_topology()->CopyFrom(current_pplan_->topology());

    for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
      new_pplan->add_stmgrs()->CopyFrom(*(iter->second->get_stmgr()));
    }
    for (sp_int32 i = 0; i < current_pplan_->instances_size(); ++i) {
      new_pplan->add_instances()->CopyFrom(current_pplan_->instances(i));
    }
    return new_pplan;
  }

  // TMaster does not really have any control over who does what.
  // That has already been decided while launching the jobs.
  // TMaster just stiches the info together to pass to everyone

  // Build the PhysicalPlan structure
  proto::system::PhysicalPlan* new_pplan = new proto::system::PhysicalPlan();
  new_pplan->mutable_topology()->CopyFrom(*topology_);

  // Build the physical assignments
  for (StMgrMapIter stmgr_iter = stmgrs_.begin(); stmgr_iter != stmgrs_.end(); ++stmgr_iter) {
    new_pplan->add_stmgrs()->CopyFrom(*(stmgr_iter->second->get_stmgr()));
    const std::vector<proto::system::Instance*>& instances = stmgr_iter->second->get_instances();
    for (size_t i = 0; i < instances.size(); ++i) {
      new_pplan->add_instances()->CopyFrom(*(instances[i]));
    }
  }

  return new_pplan;
}

proto::system::Status* TMaster::UpdateStMgrHeartbeat(Connection* _conn, sp_int64 _time,
                                                     proto::system::StMgrStats* _stats) {
  proto::system::Status* retval = new proto::system::Status();
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    retval->set_status(proto::system::INVALID_STMGR);
    retval->set_message("Uknown connection doing stmgr heartbeat");
    return retval;
  }
  const sp_string& stmgr = connection_to_stmgr_id_[_conn];
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

proto::system::StatusCode TMaster::RemoveStMgrConnection(Connection* _conn) {
  if (connection_to_stmgr_id_.find(_conn) == connection_to_stmgr_id_.end()) {
    return proto::system::INVALID_STMGR;
  }
  const sp_string& stmgr_id = connection_to_stmgr_id_[_conn];
  if (stmgrs_.find(stmgr_id) == stmgrs_.end()) {
    return proto::system::INVALID_STMGR;
  }
  StMgrState* stmgr = stmgrs_[stmgr_id];
  // This guy disconnected from us
  LOG(INFO) << "StMgr " << stmgr->get_stmgr()->id() << " disconnected from us" << std::endl;
  stmgrs_.erase(stmgr->get_id());
  connection_to_stmgr_id_.erase(_conn);
  absent_stmgrs_.insert(stmgr->get_id());
  delete stmgr;
  return proto::system::OK;
}

////////////////////////////////////////////////////////////////////////////////
// Below are valid checking functions
////////////////////////////////////////////////////////////////////////////////
bool TMaster::ValidateTopology(proto::api::Topology _topology) {
  if (tmaster_location_->topology_name() != _topology.name()) {
    LOG(ERROR) << "topology name mismatch! Expected topology name is "
               << tmaster_location_->topology_name() << " but found in zk " << _topology.name()
               << std::endl;
    return false;
  }
  if (tmaster_location_->topology_id() != _topology.id()) {
    LOG(ERROR) << "topology id mismatch! Expected topology id is "
               << tmaster_location_->topology_id() << " but found in zk " << _topology.id()
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

bool TMaster::ValidateStMgrsWithTopology(proto::api::Topology _topology) {
  // here we check to see if the total number of instances
  // accross all stmgrs match up to all the spout/bolt
  // parallelism the topology has specified
  sp_int32 ntasks = 0;
  for (sp_int32 i = 0; i < _topology.spouts_size(); ++i) {
    ntasks +=
        config::TopologyConfigHelper::GetComponentParallelism(_topology.spouts(i).comp().config());
  }
  for (sp_int32 i = 0; i < _topology.bolts_size(); ++i) {
    ntasks +=
        config::TopologyConfigHelper::GetComponentParallelism(_topology.bolts(i).comp().config());
  }

  sp_int32 ninstances = 0;
  for (StMgrMapIter iter = stmgrs_.begin(); iter != stmgrs_.end(); ++iter) {
    ninstances += iter->second->get_num_instances();
  }

  return ninstances == ntasks;
}

bool TMaster::ValidateStMgrsWithPhysicalPlan(proto::system::PhysicalPlan _pplan) {
  std::map<std::string, std::vector<proto::system::Instance*> > stmgr_to_instance_map;
  for (sp_int32 i = 0; i < _pplan.instances_size(); ++i) {
    proto::system::Instance* instance = _pplan.mutable_instances(i);
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
      return false;
    }
    if (!iter->second->VerifyInstances(stmgr_to_instance_map[iter->first])) {
      return false;
    }
  }
  return true;
}
}  // namespace tmaster
}  // namespace heron
