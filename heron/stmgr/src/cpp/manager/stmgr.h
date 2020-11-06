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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_H_

#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <chrono>
#include <typeindex>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/topology-config-vars.h"

namespace heron {
namespace common {
class HeronStateMgr;
class MetricsMgrSt;
class MultiAssignableMetric;
class CountMetric;
class MultiCountMetric;
class TimeSpentMetric;
}
}

namespace heron {
namespace stmgr {

using std::shared_ptr;

class StMgrServer;
class InstanceServer;
class StMgrClientMgr;
class TManagerClient;
class StreamConsumers;
class XorManager;
class TupleCache;
class NeighbourCalculator;
class StatefulRestorer;
class CkptMgrClient;

class StMgr {
 public:
  StMgr(shared_ptr<EventLoop> eventLoop, const sp_string& _myhost, sp_int32 _data_port,
        sp_int32 _local_data_port,
        const sp_string& _topology_name, const sp_string& _topology_id,
        shared_ptr<proto::api::Topology> _topology, const sp_string& _stmgr_id,
        const std::vector<sp_string>& _instances, const sp_string& _zkhostport,
        const sp_string& _zkroot, sp_int32 _metricsmgr_port, sp_int32 _shell_port,
        sp_int32 _ckptmgr_port, const sp_string& _ckptmgr_id,
        sp_int64 _high_watermark, sp_int64 _low_watermark,
        const sp_string& _metricscachemgr_mode);
  virtual ~StMgr();

  // All kinds of initialization like starting servers and clients
  void Init();

  // Called by tmanager client when a new physical plan is available
  void NewPhysicalPlan(shared_ptr<proto::system::PhysicalPlan> pplan);
  void HandleStreamManagerData(const sp_string& _stmgr_id,
                               pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message);
  void HandleInstanceData(sp_int32 _task_id, bool _local_spout,
                          pool_unique_ptr<proto::system::HeronTupleSet> _message);
  // Called when an instance does checkpoint and sends its checkpoint
  // to the stmgr to save it
  void HandleStoreInstanceStateCheckpoint(
      const proto::ckptmgr::InstanceStateCheckpoint& _message,
      const proto::system::Instance& _instance);
  void DrainInstanceData(sp_int32 _task_id, proto::system::HeronTupleSet2* _tuple);
  // Send checkpoint message to this task_id
  void DrainDownstreamCheckpoint(sp_int32 _task_id,
                                proto::ckptmgr::DownstreamStatefulCheckpoint* _message);

  const shared_ptr<proto::system::PhysicalPlan> GetPhysicalPlan() const;

  // Forward the call to the StmgrServer
  virtual void StartBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Forward the call to the StmgrServer
  virtual void StopBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Used by the server to tell the client to send the back pressure related
  // messages
  void SendStartBackPressureToOtherStMgrs();
  void SendStopBackPressureToOtherStMgrs();
  void StartBackPressureOnSpouts();
  void AttemptStopBackPressureFromSpouts();
  void StartTManagerClient();
  bool DidAnnounceBackPressure();
  bool DidOthersAnnounceBackPressure();
  const NetworkOptions&  GetStmgrServerNetworkOptions() const;
  const NetworkOptions&  GetInstanceServerNetworkOptions() const;
  void HandleDeadStMgrConnection(const sp_string& _stmgr);
  void HandleAllStMgrClientsRegistered();
  void HandleDeadInstance(sp_int32 _task_id);
  void HandleAllInstancesConnected();
  void HandleCkptMgrRegistration();

  // Handle checkpoint message coming from an upstream task to a downstream task
  void HandleDownStreamStatefulCheckpoint(
                                const proto::ckptmgr::DownstreamStatefulCheckpoint& _message);

  // Handle RestoreInstanceStateResponse message from local instance
  void HandleRestoreInstanceStateResponse(sp_int32 _task_id, const proto::system::Status& _status,
                                          const std::string& _checkpoint_id);

 private:
  void OnTManagerLocationFetch(shared_ptr<proto::tmanager::TManagerLocation> _tmanager,
          proto::system::StatusCode);
  void OnMetricsCacheLocationFetch(
         shared_ptr<proto::tmanager::MetricsCacheLocation> _tmanager, proto::system::StatusCode);
  void FetchTManagerLocation();
  void FetchMetricsCacheLocation();
  // A wrapper that calls FetchTManagerLocation. Needed for RegisterTimer
  void CheckTManagerLocation(EventLoop::Status);

  void UpdateUptimeMetric();
  void UpdateProcessMetrics(EventLoop::Status);

  // Utility function to create checkpoint mgr client
  void CreateCheckpointMgrClient();
  // Called when ckpt mgr saves the state of an instance
  void HandleSavedInstanceState(const proto::system::Instance& _instance,
                                const std::string& _checkpoint_id);
  // Called when ckpt mgr reteives the state of an instance
  void HandleGetInstanceState(proto::system::StatusCode _status, sp_int32 _task_id,
                              sp_string _checkpoint_id,
                              const proto::ckptmgr::InstanceStateCheckpoint& _msg);

  void CleanupStreamConsumers();
  void PopulateStreamConsumers(
      proto::api::Topology* _topology,
      const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids);
  void PopulateXorManagers(
      const proto::api::Topology& _topology, sp_int32 _message_timeout,
      const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids);
  void CleanupXorManagers();

  void SendInBound(sp_int32 _task_id, proto::system::HeronTupleSet2* _message);
  void ProcessAcksAndFails(sp_int32 _src_task_id,
                           sp_int32 _task_id, const proto::system::HeronControlTupleSet& _control);
  void CopyDataOutBound(sp_int32 _src_task_id, bool _local_spout,
                        const proto::api::StreamId& _streamid,
                        proto::system::HeronDataTuple* _tuple,
                        const std::vector<sp_int32>& _out_tasks);
  void CopyControlOutBound(sp_int32 _src_task_id,
                           const proto::system::AckTuple& _control, bool _is_fail);

  sp_int32 ExtractTopologyTimeout(const proto::api::Topology& _topology);

  void CreateTManagerClient(shared_ptr<proto::tmanager::TManagerLocation> tmanagerLocation);
  void StartStmgrServer();
  void StartInstanceServer();
  void CreateTupleCache();
  // This is called when we receive a valid new Tmanager Location.
  // Performs all the actions necessary to deal with new tmanager.
  void HandleNewTmanager(shared_ptr<proto::tmanager::TManagerLocation> newTmanagerLocation);
  // Broadcast the tmanager location changes to other components. (MM for now)
  void BroadcastTmanagerLocation(shared_ptr<proto::tmanager::TManagerLocation> tmanagerLocation);
  void BroadcastMetricsCacheLocation(
          shared_ptr<proto::tmanager::MetricsCacheLocation> tmanagerLocation);

  // Called when TManager sends a InitiateStatefulCheckpoint message with a checkpoint_id
  // This will send intiate checkpoint messages to local instances to capture their state.
  void InitiateStatefulCheckpoint(sp_string checkpoint_id);

  // Invoked when TManager asks us to restore all our local instances state to
  // the checkpoint represented by _checkpoint_id. This starts the
  // Restore state machine
  void RestoreTopologyState(sp_string _checkpoint_id, sp_int64 _restore_txid);

  // Invoked when TManager sends the StartStatefulProcessing request to kick
  // start the computation. We send the StartStatefulProcessing to all our
  // local instances so that they can start the processing.
  void StartStatefulProcessing(sp_string _checkpoint_id);

  // Called when Stateful Restorer restores the instance state
  void HandleStatefulRestoreDone(proto::system::StatusCode _status,
                                 std::string _checkpoint_id, sp_int64 _restore_txid);

  // Called when stmgr received StatefulConsistentCheckpointSaved message from TManager
  // Then, the stmgr will forward this fact to all heron instances connected to it
  void BroadcastCheckpointSaved(const proto::ckptmgr::StatefulConsistentCheckpointSaved& _msg);

  // Patch new physical plan with internal hydrated topology but keep new topology data:
  // - new topology state
  // - new topology/component config
  static void PatchPhysicalPlanWithHydratedTopology(shared_ptr<proto::system::PhysicalPlan> _pplan,
                                                    proto::api::Topology const& _topology);

  shared_ptr<heron::common::HeronStateMgr> state_mgr_;
  shared_ptr<proto::system::PhysicalPlan> pplan_;
  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  sp_string stmgr_host_;
  sp_int32 data_port_;
  sp_int32 local_data_port_;
  std::vector<sp_string> instances_;
  // Getting data from other streammgrs
  shared_ptr<StMgrServer> stmgr_server_;
  // Used to get/send data to local instances
  shared_ptr<InstanceServer> instance_server_;
  // Pushing data to other streammanagers
  shared_ptr<StMgrClientMgr> clientmgr_;
  shared_ptr<TManagerClient> tmanager_client_;
  shared_ptr<EventLoop> eventLoop_;

  // Map of task_id to stmgr_id
  std::unordered_map<sp_int32, sp_string> task_id_to_stmgr_;
  // map of <component, streamid> to its consumers
  std::unordered_map<std::pair<sp_string, sp_string>,
                     unique_ptr<StreamConsumers>> stream_consumers_;
  // xor managers
  shared_ptr<XorManager> xor_mgrs_;
  // Tuple Cache to optimize message building
  shared_ptr<TupleCache> tuple_cache_;
  // Neighbour Calculator for stateful processing
  shared_ptr<NeighbourCalculator> neighbour_calculator_;
  // Stateful Restorer
  shared_ptr<StatefulRestorer> stateful_restorer_;

  // This is the topology structure
  // that contains the full component objects
  shared_ptr<proto::api::Topology> hydrated_topology_;

  // Metrics Manager
  shared_ptr<heron::common::MetricsMgrSt> metrics_manager_client_;

  // Checkpoint Manager
  shared_ptr<CkptMgrClient> ckptmgr_client_;

  // Process related metrics
  shared_ptr<heron::common::MultiAssignableMetric> stmgr_process_metrics_;

  // Stateful Restore metric
  shared_ptr<heron::common::CountMetric> restore_initiated_metrics_;
  shared_ptr<heron::common::MultiCountMetric> dropped_during_restore_metrics_;

  // Instance related metrics
  shared_ptr<heron::common::MultiCountMetric> instance_bytes_received_metrics_;

  // Backpressure relarted metrics
  shared_ptr<heron::common::TimeSpentMetric> back_pressure_metric_initiated_;

  // The time at which the stmgr was started up
  std::chrono::high_resolution_clock::time_point start_time_;
  sp_string zkhostport_;
  sp_string zkroot_;
  sp_int32 metricsmgr_port_;
  sp_int32 shell_port_;
  sp_int32 ckptmgr_port_;
  sp_string ckptmgr_id_;

  std::vector<sp_int32> out_tasks_;

  bool is_acking_enabled;
  config::TopologyConfigVars::TopologyReliabilityMode reliability_mode_;

  sp_int64 high_watermark_;
  sp_int64 low_watermark_;

  // whether MetricsCacheMgr is running
  sp_string metricscachemgr_mode_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_H_
