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

#ifndef __TMANAGER_H
#define __TMANAGER_H

#include <map>
#include <set>
#include <string>
#include <vector>

#include "statemgr/heron-statemgr.h"
#include "metrics/metrics-mgr-st.h"
#include "metrics/metrics.h"
#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmanager {

using std::unique_ptr;
using std::shared_ptr;

class StMgrState;
class TController;
class StatsInterface;
class TManagerServer;
class TMetricsCollector;
class StatefulController;
class CkptMgrClient;

typedef std::map<std::string, shared_ptr<StMgrState>> StMgrMap;
typedef StMgrMap::iterator StMgrMapIter;

typedef std::map<std::string, std::string> ConfigValueMap;
// From component name to config/value pairs
typedef std::map<std::string, std::map<std::string, std::string>> ComponentConfigMap;

class TManager {
 public:
  TManager(const std::string& _zk_hostport, const std::string& _topology_name,
          const std::string& _topology_id, const std::string& _topdir,
          sp_int32 _tmanager_controller_port, sp_int32 _server_port,
          sp_int32 _stats_port, sp_int32 metricsMgrPort, sp_int32 _ckptmgr_port,
          const std::string& metrics_sinks_yaml,
          const std::string& _myhost_name, shared_ptr<EventLoop> eventLoop);

  virtual ~TManager();

  const std::string& GetTopologyId() const { return current_pplan_->topology().id(); }
  const std::string& GetTopologyName() const { return current_pplan_->topology().name(); }
  proto::api::TopologyState GetTopologyState() const { return current_pplan_->topology().state(); }

  void ActivateTopology(VCallback<proto::system::StatusCode> cb);
  void DeActivateTopology(VCallback<proto::system::StatusCode> cb);
  // Update runtime configs in a topology.
  // Return true if successful; false otherwise and the callback function won't be invoked.
  bool UpdateRuntimeConfig(const ComponentConfigMap& _config,
                           VCallback<proto::system::StatusCode> cb);
  // Validate runtime config. Return false if any issue is found.
  bool ValidateRuntimeConfig(const ComponentConfigMap& _config) const;

  proto::system::Status* RegisterStMgr(const proto::system::StMgr& _stmgr,
                                const std::vector<shared_ptr<proto::system::Instance>>& _instances,
                                Connection* _conn, shared_ptr<proto::system::PhysicalPlan>& _pplan);
  // function to update heartbeat for a nodemgr
  proto::system::Status* UpdateStMgrHeartbeat(Connection* _conn, sp_int64 _time,
                                              proto::system::StMgrStats* _stats);

  // When stmgr disconnects from us
  proto::system::StatusCode RemoveStMgrConnection(Connection* _conn);

  // Called by http server upon receiving a user message to cleanup the state
  void CleanAllStatefulCheckpoint();
  // Called by ckptmgr client upon receiving CleanStatefulCheckpointResponse
  void HandleCleanStatefulCheckpointResponse(proto::system::StatusCode);

  // Get stream managers registration summary
  std::unique_ptr<proto::tmanager::StmgrsRegistrationSummaryResponse> GetStmgrsRegSummary();

  // Accessors
  const shared_ptr<proto::system::PhysicalPlan> getPhysicalPlan() const { return current_pplan_; }
  // TODO(mfu): Should we provide this?
  // topology_ should only be used to construct physical plan when TManager first starts
  // Providing an accessor is bug prone.
  // Now used in GetMetrics function in tmetrics-collector
  const proto::api::Topology& getInitialTopology() const { return *topology_; }

  // Timer function to start the stateful checkpoint process
  void SendCheckpointMarker();

  // Called by tmanager server when it gets InstanceStateStored message
  void HandleInstanceStateStored(const std::string& _checkpoint_id,
                                 const proto::system::Instance& _instance);

  // Called by tmanager server when it gets RestoreTopologyStateResponse message
  void HandleRestoreTopologyStateResponse(Connection* _conn,
                                          const std::string& _checkpoint_id,
                                          int64_t _restore_txid,
                                          proto::system::StatusCode _status);

  // Called by tmanager server when it gets ResetTopologyState message
  void ResetTopologyState(Connection* _conn, const std::string& _dead_stmgr,
                          int32_t _dead_instance, const std::string& _reason);

 private:
  // Helper function to fetch physical plan
  void FetchPhysicalPlan();

  // Function to be called that calls MakePhysicalPlan and sends it to all stmgrs
  void DoPhysicalPlan(EventLoop::Status _code);

  // Log config object
  void LogConfig(const ComponentConfigMap& _config);

  // Big brother function that does the assignment to the workers
  // If _new_stmgr is null, this means that there was a plan
  // existing, but a _new_stmgr joined us. So redo his part
  // If _new_stmgr is empty, this means do pplan from scratch
  shared_ptr<proto::system::PhysicalPlan> MakePhysicalPlan();

  // Check to see if the topology is of correct format
  bool ValidateTopology(const proto::api::Topology& _topology);

  // Check to see if the topology and stmgrs match
  // in terms of workers
  bool ValidateStMgrsWithPackingPlan();

  // Check to see if the stmgrs and pplan match
  // in terms of workers
  bool ValidateStMgrsWithPhysicalPlan(shared_ptr<proto::system::PhysicalPlan> _pplan);

  // Check if incoming runtime configs are valid or not.
  // All incoming configurations must exist. If there is any non-existing
  // configuration, or the data type is wrong, return false.
  bool ValidateRuntimeConfigNames(const ComponentConfigMap& _config) const;

  // If the assignment is already done, then:
  // 1. Distribute physical plan to all active stmgrs
  bool DistributePhysicalPlan();

  // Function called after we set the tmanagerlocation
  void SetTManagerLocationDone(proto::system::StatusCode _code);
  // Function called after we get the topology
  void GetTopologyDone(proto::system::StatusCode _code);

  // Function called after we get StatefulConsistentCheckpoints
  void GetStatefulCheckpointsDone(shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt,
                                  proto::system::StatusCode _code);
  // Function called after we set an initial StatefulConsistentCheckpoints
  void SetStatefulCheckpointsDone(proto::system::StatusCode _code,
                                  shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt);
  // Helper function to setup stateful coordinator
  void SetupStatefulController(shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt);

  // Function called after we try to get assignment
  void GetPhysicalPlanDone(shared_ptr<proto::system::PhysicalPlan> _pplan,
          proto::system::StatusCode _code);

  // Function called after we try to commit a new assignment
  void SetPhysicalPlanDone(shared_ptr<proto::system::PhysicalPlan> _pplan,
                           proto::system::StatusCode _code);

  // Function called when we want to setup ourselves as tmanager
  void EstablishTManager(EventLoop::Status);

  void EstablishPackingPlan(EventLoop::Status);
  void FetchPackingPlan();
  void OnPackingPlanFetch(shared_ptr<proto::system::PackingPlan> newPackingPlan,
                          proto::system::StatusCode _status);

  // Metrics updates
  void UpdateUptimeMetric();
  void UpdateProcessMetrics(EventLoop::Status);

  // Update configurations in physical plan.
  bool UpdateRuntimeConfigInTopology(proto::api::Topology* _topology,
                                     const ComponentConfigMap& _config);

  // Function called when a new stateful ckpt record is saved
  void HandleStatefulCheckpointSave(
      const proto::ckptmgr::StatefulConsistentCheckpoints &new_ckpts);

  // Function called to kill container
  void KillContainer(const std::string& host_name,
                     sp_int32 port,
                     const std::string& stmgr_id);

  // map of active stmgr id to stmgr state
  StMgrMap stmgrs_;

  // map of connection to stmgr id
  std::map<Connection*, std::string> connection_to_stmgr_id_;

  // set of nodemanagers that have not yet connected to us
  std::set<std::string> absent_stmgrs_;

  // The current physical plan
  shared_ptr<proto::system::PhysicalPlan> current_pplan_;

  // The topology as first submitted by the user
  // It shall only be used to construct the physical plan when TManager first time starts
  // Any runtime changes shall be made to current_pplan_->topology
  unique_ptr<proto::api::Topology> topology_;

  shared_ptr<proto::system::PackingPlan> packing_plan_;

  // The statemgr where we store/retrieve our state
  shared_ptr<heron::common::HeronStateMgr> state_mgr_;

  // Our copy of the tmanagerlocation
  unique_ptr<proto::tmanager::TManagerLocation> tmanager_location_;

  // When we are in the middle of doing assignment
  // we set this to true
  bool assignment_in_progress_;
  bool do_reassign_;

  // State information
  std::string zk_hostport_;
  std::string topdir_;

  // Servers that implement our services
  unique_ptr<TController> tmanager_controller_;
  sp_int32 tmanager_controller_port_;
  unique_ptr<TManagerServer> server_;
  sp_int32 server_port_;
  unique_ptr<StatsInterface> stats_;
  sp_int32 stats_port_;
  std::string myhost_name_;

  // how many times have we tried to establish
  // ourselves as server
  sp_int32 server_establish_attempts_;

  // collector
  shared_ptr<TMetricsCollector> metrics_collector_;

  sp_int32 mMetricsMgrPort;
  // Metrics Manager
  shared_ptr<heron::common::MetricsMgrSt> mMetricsMgrClient;

  // Ckpt Manager
  unique_ptr<CkptMgrClient> ckptmgr_client_;
  sp_int32 ckptmgr_port_;

  // Process related metrics
  shared_ptr<heron::common::MultiAssignableMetric> tmanagerProcessMetrics;

  // The time at which the stmgr was started up
  std::chrono::high_resolution_clock::time_point start_time_;

  // Stateful Controller
  unique_ptr<StatefulController> stateful_controller_;

  // HTTP client
  AsyncDNS* dns_;
  HTTPClient* http_client_;

  // Copy of the EventLoop
  shared_ptr<EventLoop> eventLoop_;
};
}  // namespace tmanager
}  // namespace heron

#endif
