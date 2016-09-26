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
#include <unordered_map>
#include <utility>
#include <vector>
#include <chrono>
#include <typeindex>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class HeronStateMgr;
class MetricsMgrSt;
class MultiAssignableMetric;
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

class StMgr {
 public:
  StMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
        const sp_string& _topology_id, proto::api::Topology* _topology, const sp_string& _stmgr_id,
        const std::vector<sp_string>& _instances, const sp_string& _zkhostport,
        const sp_string& _zkroot, sp_int32 _metricsmgr_port, sp_int32 _shell_port);
  virtual ~StMgr();

  // All kinds of initialization like starting servers and clients
  void Init();

  // Called by tmaster client when a new physical plan is available
  void NewPhysicalPlan(proto::system::PhysicalPlan* pplan);
  void HandleStreamManagerData(const sp_string& _stmgr_id,
                               const proto::stmgr::TupleStreamMessage2& _message);
  void HandleInstanceData(sp_int32 _task_id, bool _local_spout,
                          proto::system::HeronTupleSet* _message);
  void DrainInstanceData(sp_int32 _task_id, proto::system::HeronTupleSet2* _tuple);
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

 private:
  void OnTMasterLocationFetch(proto::tmaster::TMasterLocation* _tmaster, proto::system::StatusCode);
  void FetchTMasterLocation();
  // A wrapper that calls FetchTMasterLocation. Needed for RegisterTimer
  void CheckTMasterLocation(EventLoop::Status);
  void UpdateProcessMetrics(EventLoop::Status);

  void CleanupStreamConsumers();
  void PopulateStreamConsumers(
      proto::api::Topology* _topology,
      const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids);
  void PopulateXorManagers(
      const proto::api::Topology& _topology, sp_int32 _message_timeout,
      const std::map<sp_string, std::vector<sp_int32> >& _component_to_task_ids);
  void CleanupXorManagers();

  void SendInBound(sp_int32 _task_id, proto::system::HeronTupleSet2* _message);
  void ProcessAcksAndFails(sp_int32 _task_id, const proto::system::HeronControlTupleSet& _control);
  void CopyDataOutBound(sp_int32 _src_task_id, bool _local_spout,
                        const proto::api::StreamId& _streamid,
                        proto::system::HeronDataTuple* _tuple,
                        const std::vector<sp_int32>& _out_tasks);
  void CopyControlOutBound(const proto::system::AckTuple& _control, bool _is_fail);

  sp_int32 ExtractTopologyTimeout(const proto::api::Topology& _topology);

  void CreateTMasterClient(proto::tmaster::TMasterLocation* tmasterLocation);
  void StartStmgrServer();
  void CreateTupleCache();
  // This is called when we receive a valid new Tmaster Location.
  // Performs all the actions necessary to deal with new tmaster.
  void HandleNewTmaster(proto::tmaster::TMasterLocation* newTmasterLocation);
  // Broadcast the tmaster location changes to other components. (MM for now)
  void BroadcastTmasterLocation(proto::tmaster::TMasterLocation* tmasterLocation);

  heron::common::HeronStateMgr* state_mgr_;
  proto::system::PhysicalPlan* pplan_;
  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
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

  // This is the topology structure
  // that contains the full component objects
  proto::api::Topology* hydrated_topology_;

  // Metrics Manager
  heron::common::MetricsMgrSt* metrics_manager_client_;

  // Process related metrics
  heron::common::MultiAssignableMetric* stmgr_process_metrics_;

  // The time at which the stmgr was started up
  std::chrono::high_resolution_clock::time_point start_time_;
  sp_string zkhostport_;
  sp_string zkroot_;
  sp_int32 metricsmgr_port_;
  sp_int32 shell_port_;

  proto::system::HeronTupleSet2 current_control_tuple_set_;
  std::vector<sp_int32> out_tasks_;

  bool is_acking_enabled;

  proto::system::HeronTupleSet2* tuple_set_from_other_stmgr_;

  sp_string heron_tuple_set_2_ = "heron.proto.system.HeronTupleSet2";
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_H_
