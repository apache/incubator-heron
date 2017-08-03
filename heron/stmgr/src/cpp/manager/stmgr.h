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
}
}

namespace heron {
namespace stmgr {

class StMgrServer;
class StMgrClientMgr;
class TMasterClient;
class StreamConsumers;
class XorManager;
class TupleCache;
class NeighbourCalculator;
class StatefulRestorer;
class CkptMgrClient;

class StMgr {
 public:
  StMgr(EventLoop* eventLoop, const sp_string& _myhost, sp_int32 _myport,
        const sp_string& _topology_name, const sp_string& _topology_id,
        proto::api::Topology* _topology, const sp_string& _stmgr_id,
        const std::vector<sp_string>& _instances, const sp_string& _zkhostport,
        const sp_string& _zkroot, sp_int32 _metricsmgr_port, sp_int32 _shell_port,
        sp_int32 _ckptmgr_port, const sp_string& _ckptmgr_id,
        sp_int64 _high_watermark, sp_int64 _low_watermark);
  virtual ~StMgr();

  // All kinds of initialization like starting servers and clients
  void Init();

  // Called by tmaster client when a new physical plan is available
  void NewPhysicalPlan(proto::system::PhysicalPlan* pplan);
  void HandleStreamManagerData(const sp_string& _stmgr_id,
                               proto::stmgr::TupleStreamMessage* _message);
  void HandleInstanceData(sp_int32 _task_id, bool _local_spout,
                          proto::system::HeronTupleSet* _message);
  // Called when an instance does checkpoint and sends its checkpoint
  // to the stmgr to save it
  void HandleStoreInstanceStateCheckpoint(const proto::ckptmgr::InstanceStateCheckpoint& _message,
                                          const proto::system::Instance& _instance);
  void DrainInstanceData(sp_int32 _task_id, proto::system::HeronTupleSet2* _tuple);
  // Send checkpoint message to this task_id
  void DrainDownstreamCheckpoint(sp_int32 _task_id,
                                proto::ckptmgr::DownstreamStatefulCheckpoint* _message);

  const proto::system::PhysicalPlan* GetPhysicalPlan() const;

  // Forward the call to the StmgrServer
  virtual void StartBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Forward the call to the StmgrServer
  virtual void StopBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Used by the server to tell the client to send the back pressure related
  // messages
  void SendStartBackPressureToOtherStMgrs();
  void SendStopBackPressureToOtherStMgrs();
  void StartTMasterClient();
  bool DidAnnounceBackPressure();
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
  void OnTMasterLocationFetch(proto::tmaster::TMasterLocation* _tmaster, proto::system::StatusCode);
  void OnMetricsCacheLocationFetch(
         proto::tmaster::MetricsCacheLocation* _tmaster, proto::system::StatusCode);
  void FetchTMasterLocation();
  void FetchMetricsCacheLocation();
  // A wrapper that calls FetchTMasterLocation. Needed for RegisterTimer
  void CheckTMasterLocation(EventLoop::Status);
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

  void CreateTMasterClient(proto::tmaster::TMasterLocation* tmasterLocation);
  void StartStmgrServer();
  void CreateTupleCache();
  // This is called when we receive a valid new Tmaster Location.
  // Performs all the actions necessary to deal with new tmaster.
  void HandleNewTmaster(proto::tmaster::TMasterLocation* newTmasterLocation);
  // Broadcast the tmaster location changes to other components. (MM for now)
  void BroadcastTmasterLocation(proto::tmaster::TMasterLocation* tmasterLocation);
  void BroadcastMetricsCacheLocation(proto::tmaster::MetricsCacheLocation* tmasterLocation);

  // Called when TMaster sends a InitiateStatefulCheckpoint message with a checkpoint_id
  // This will send intiate checkpoint messages to local instances to capture their state.
  void InitiateStatefulCheckpoint(sp_string checkpoint_id);

  // Invoked when TMaster asks us to restore all our local instances state to
  // the checkpoint represented by _checkpoint_id. This starts the
  // Restore state machine
  void RestoreTopologyState(sp_string _checkpoint_id, sp_int64 _restore_txid);

  // Invoked when TMaster sends the StartStatefulProcessing request to kick
  // start the computation. We send the StartStatefulProcessing to all our
  // local instances so that they can start the processing.
  void StartStatefulProcessing(sp_string _checkpoint_id);

  // Called when Stateful Restorer restores the instance state
  void HandleStatefulRestoreDone(proto::system::StatusCode _status,
                                 std::string _checkpoint_id, sp_int64 _restore_txid);

  heron::common::HeronStateMgr* state_mgr_;
  proto::system::PhysicalPlan* pplan_;
  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  sp_string stmgr_host_;
  sp_int32 stmgr_port_;
  std::vector<sp_string> instances_;
  // Getting data from other streammgrs
  // Also used to get/send data to local instances
  StMgrServer* server_;
  // Pushing data to other streammanagers
  StMgrClientMgr* clientmgr_;
  TMasterClient* tmaster_client_;
  EventLoop* eventLoop_;

  // Map of task_id to stmgr_id
  std::unordered_map<sp_int32, sp_string> task_id_to_stmgr_;
  // map of <component, streamid> to its consumers
  std::unordered_map<std::pair<sp_string, sp_string>, StreamConsumers*> stream_consumers_;
  // xor managers
  XorManager* xor_mgrs_;
  // Tuple Cache to optimize message building
  TupleCache* tuple_cache_;
  // Neighbour Calculator for stateful processing
  NeighbourCalculator* neighbour_calculator_;
  // Stateful Restorer
  StatefulRestorer* stateful_restorer_;

  // This is the topology structure
  // that contains the full component objects
  proto::api::Topology* hydrated_topology_;

  // Metrics Manager
  heron::common::MetricsMgrSt* metrics_manager_client_;

  // Checkpoint Manager
  CkptMgrClient* ckptmgr_client_;

  // Process related metrics
  heron::common::MultiAssignableMetric* stmgr_process_metrics_;

  // Stateful Restore metric
  heron::common::CountMetric* restore_initiated_metrics_;
  heron::common::MultiCountMetric* dropped_during_restore_metrics_;

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
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_H_
