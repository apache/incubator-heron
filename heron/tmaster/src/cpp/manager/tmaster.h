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

#ifndef __TMASTER_H
#define __TMASTER_H

#include <map>
#include <set>
#include <string>
#include <vector>
#include "statemgr/heron-statemgr.h"
#include "metrics/metrics-mgr-st.h"
#include "metrics/metrics.h"
#include "network/network.h"
#include "proto/tmaster.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmaster {

class StMgrState;
class TController;
class StatsInterface;
class TMasterServer;
class TMetricsCollector;
class StatefulController;
class CkptMgrClient;

typedef std::map<std::string, StMgrState*> StMgrMap;
typedef StMgrMap::iterator StMgrMapIter;

class TMaster {
 public:
  TMaster(const std::string& _zk_hostport, const std::string& _topology_name,
          const std::string& _topology_id, const std::string& _topdir,
          sp_int32 _tmaster_controller_port, sp_int32 _master_port,
          sp_int32 _stats_port, sp_int32 metricsMgrPort, sp_int32 _ckptmgr_port,
          const std::string& metrics_sinks_yaml,
          const std::string& _myhost_name, EventLoop* eventLoop);

  virtual ~TMaster();

  const std::string& GetTopologyId() const { return current_pplan_->topology().id(); }
  const std::string& GetTopologyName() const { return current_pplan_->topology().name(); }
  proto::api::TopologyState GetTopologyState() const { return current_pplan_->topology().state(); }
  void ActivateTopology(VCallback<proto::system::StatusCode> cb);
  void DeActivateTopology(VCallback<proto::system::StatusCode> cb);
  proto::system::Status* RegisterStMgr(const proto::system::StMgr& _stmgr,
                                       const std::vector<proto::system::Instance*>& _instances,
                                       Connection* _conn, proto::system::PhysicalPlan*& _pplan);
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
  proto::tmaster::StmgrsRegistrationSummaryResponse* GetStmgrsRegSummary();

  // Accessors
  const proto::system::PhysicalPlan* getPhysicalPlan() const { return current_pplan_; }
  // TODO(mfu): Should we provide this?
  // topology_ should only be used to construct physical plan when TMaster first starts
  // Providing an accessor is bug prone.
  // Now used in GetMetrics function in tmetrics-collector
  const proto::api::Topology* getInitialTopology() const { return topology_; }

  // Timer function to start the stateful checkpoint process
  void SendCheckpointMarker();

  // Called by tmaster server when it gets InstanceStateStored message
  void HandleInstanceStateStored(const std::string& _checkpoint_id,
                                 const proto::system::Instance& _instance);

  // Called by tmaster server when it gets RestoreTopologyStateResponse message
  void HandleRestoreTopologyStateResponse(Connection* _conn,
                                          const std::string& _checkpoint_id,
                                          int64_t _restore_txid,
                                          proto::system::StatusCode _status);

  // Called by tmaster server when it gets ResetTopologyState message
  void ResetTopologyState(Connection* _conn, const std::string& _dead_stmgr,
                          int32_t _dead_instance, const std::string& _reason);

 private:
  // Helper function to fetch physical plan
  void FetchPhysicalPlan();

  // Function to be called that calls MakePhysicalPlan and sends it to all stmgrs
  void DoPhysicalPlan(EventLoop::Status _code);

  // Big brother function that does the assignment to the workers
  // If _new_stmgr is null, this means that there was a plan
  // existing, but a _new_stmgr joined us. So redo his part
  // If _new_stmgr is empty, this means do pplan from scratch
  proto::system::PhysicalPlan* MakePhysicalPlan();

  // Check to see if the topology is of correct format
  bool ValidateTopology(proto::api::Topology _topology);

  // Check to see if the topology and stmgrs match
  // in terms of workers
  bool ValidateStMgrsWithTopology(proto::api::Topology _topology);

  // Check to see if the stmgrs and pplan match
  // in terms of workers
  bool ValidateStMgrsWithPhysicalPlan(proto::system::PhysicalPlan _pplan);

  // If the assignment is already done, then:
  // 1. Distribute physical plan to all active stmgrs
  bool DistributePhysicalPlan();

  // Function called after we set the tmasterlocation
  void SetTMasterLocationDone(proto::system::StatusCode _code);
  // Function called after we get the topology
  void GetTopologyDone(proto::system::StatusCode _code);

  // Function called after we get StatefulConsistentCheckpoints
  void GetStatefulCheckpointsDone(proto::ckptmgr::StatefulConsistentCheckpoints* _ckpt,
                                  proto::system::StatusCode _code);
  // Function called after we set an initial StatefulConsistentCheckpoints
  void SetStatefulCheckpointsDone(proto::system::StatusCode _code,
                            proto::ckptmgr::StatefulConsistentCheckpoints* _ckpt);
  // Helper function to setup stateful coordinator
  void SetupStatefulController(proto::ckptmgr::StatefulConsistentCheckpoints* _ckpt);

  // Function called after we try to get assignment
  void GetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan, proto::system::StatusCode _code);

  // Function called after we try to commit a new assignment
  void SetPhysicalPlanDone(proto::system::PhysicalPlan* _pplan, proto::system::StatusCode _code);

  // Function called when we want to setup ourselves as tmaster
  void EstablishTMaster(EventLoop::Status);

  void EstablishPackingPlan(EventLoop::Status);
  void FetchPackingPlan();
  void OnPackingPlanFetch(proto::system::PackingPlan* newPackingPlan,
                          proto::system::StatusCode _status);

  void UpdateProcessMetrics(EventLoop::Status);

  // Function called when a new stateful ckpt record is saved
  void HandleStatefulCheckpointSave(std::string _oldest_ckpt);

  // map of active stmgr id to stmgr state
  StMgrMap stmgrs_;

  // map of connection to stmgr id
  std::map<Connection*, sp_string> connection_to_stmgr_id_;

  // set of nodemanagers that have not yet connected to us
  std::set<std::string> absent_stmgrs_;

  // The current physical plan
  proto::system::PhysicalPlan* current_pplan_;

  // The topology as first submitted by the user
  // It shall only be used to construct the physical plan when TMaster first time starts
  // Any runtime changes shall be made to current_pplan_->topology
  proto::api::Topology* topology_;

  proto::system::PackingPlan* packing_plan_;

  // The statemgr where we store/retrieve our state
  heron::common::HeronStateMgr* state_mgr_;

  // Our copy of the tmasterlocation
  proto::tmaster::TMasterLocation* tmaster_location_;

  // When we are in the middle of doing assignment
  // we set this to true
  bool assignment_in_progress_;
  bool do_reassign_;

  // State information
  std::string zk_hostport_;
  std::string topdir_;

  // Servers that implement our services
  TController* tmaster_controller_;
  sp_int32 tmaster_controller_port_;
  TMasterServer* master_;
  sp_int32 master_port_;
  StatsInterface* stats_;
  sp_int32 stats_port_;
  std::string myhost_name_;

  // how many times have we tried to establish
  // ourselves as master
  sp_int32 master_establish_attempts_;

  // collector
  TMetricsCollector* metrics_collector_;

  sp_int32 mMetricsMgrPort;
  // Metrics Manager
  heron::common::MetricsMgrSt* mMetricsMgrClient;

  // Ckpt Manager
  CkptMgrClient* ckptmgr_client_;
  sp_int32 ckptmgr_port_;

  // Process related metrics
  heron::common::MultiAssignableMetric* tmasterProcessMetrics;

  // The time at which the stmgr was started up
  std::chrono::high_resolution_clock::time_point start_time_;

  // Stateful Controller
  StatefulController* stateful_controller_;

  // Copy of the EventLoop
  EventLoop* eventLoop_;
};
}  // namespace tmaster
}  // namespace heron

#endif
