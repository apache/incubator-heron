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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_INSTANCE_SERVER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_INSTANCE_SERVER_H_

#include <unordered_map>
#include <unordered_set>
#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class AssignableMetric;
class MetricsMgrSt;
class MultiCountMetric;
class MultiMeanMetric;
class TimeSpentMetric;
}
}

namespace heron {
namespace stmgr {

using std::shared_ptr;
using std::unordered_map;

class StMgr;
class NeighbourCalculator;
class CheckpointGateway;

class InstanceServer : public Server {
 public:
  InstanceServer(shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
             const sp_string& _topology_name, const sp_string& _topology_id,
             const sp_string& _stmgr_id,
             const std::vector<sp_string>& _expected_instances,
             StMgr* _stmgr,
             shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client,
             shared_ptr<NeighbourCalculator> _neighbour_calculator,
             bool _droptuples_upon_backpressure);
  virtual ~InstanceServer();

  // We own the message
  void SendToInstance2(sp_int32 _task_id, proto::system::HeronTupleSet2* _message);
  void SendToInstance2(pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message);

  // When we get a checkpoint marker from _src_task_id destined for _destination_task_id
  // this function in invoked, so that we might register it in gateway
  void HandleCheckpointMarker(sp_int32 _src_task_id, sp_int32 _destination_task_id,
                              const sp_string& _checkpoint_id);

  void BroadcastNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  void BroadcastStatefulCheckpointSaved(
      const proto::ckptmgr::StatefulConsistentCheckpointSaved& _msg);

  virtual bool HaveAllInstancesConnectedToUs() const {
    return active_instances_.size() == expected_instances_.size();
  }

  // Gets all the Instance information
  virtual void GetInstanceInfo(std::vector<proto::system::Instance*>& _return);
  // Get instance info for this task_id
  virtual proto::system::Instance* GetInstanceInfo(sp_int32 _task_id);

  bool DidAnnounceBackPressure() { return !remote_ends_who_caused_back_pressure_.empty(); }

  // Send messages to all local spouts to start the process of checkpointing
  void InitiateStatefulCheckpoint(const sp_string& _checkpoint_tag);
  // Send a RestoreInstanceStateRequest to _task_id asking it to restore itself from _state
  virtual bool SendRestoreInstanceStateRequest(sp_int32 _task_id,
      const proto::ckptmgr::InstanceStateCheckpoint& _state);
  // Send StartInstanceStatefulProcessing message to all instances so that they can start
  // processing
  void SendStartInstanceStatefulProcessing(const std::string& _ckpt_id);
  // Clears all buffered state in stateful-gateway
  virtual void ClearCache();

  // Can we free the back pressure on the spouts?
  void AttemptStopBackPressureFromSpouts();
  // Start back pressure on the spouts
  void StartBackPressureOnSpouts();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  void DrainTupleSet(sp_int32 _task_id, proto::system::HeronTupleSet2* _message);
  void DrainTupleStream(proto::stmgr::TupleStreamMessage* _message);
  void DrainCheckpoint(sp_int32 _task_id,
                       pool_unique_ptr<proto::ckptmgr::InitiateStatefulCheckpoint> _message);
  sp_string MakeBackPressureCompIdMetricName(const sp_string& instanceid);
  sp_string MakeQueueSizeCompIdMetricName(const sp_string& instanceid);
  sp_string MakeQueueLengthCompIdMetricName(const sp_string& instanceid);
  sp_string GetInstanceName(Connection* _connection);
  void UpdateQueueMetrics(EventLoop::Status);

  // Various handlers for different requests

  // Next from local instances
  void HandleRegisterInstanceRequest(REQID _id, Connection* _conn,
                                   pool_unique_ptr<proto::stmgr::RegisterInstanceRequest> _request);
  void HandleTupleSetMessage(Connection* _conn,
                             pool_unique_ptr<proto::system::HeronTupleSet> _message);
  void HandleStoreInstanceStateCheckpointMessage(Connection* _conn,
                           pool_unique_ptr<proto::ckptmgr::StoreInstanceStateCheckpoint> _message);
  void HandleRestoreInstanceStateResponse(Connection* _conn,
                            pool_unique_ptr<proto::ckptmgr::RestoreInstanceStateResponse> _message);

  // Back pressure related connection callbacks
  // Do back pressure
  virtual void StartBackPressureConnectionCb(Connection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  // Compute the LocalSpouts from Physical Plan
  void ComputeLocalSpouts(const proto::system::PhysicalPlan& _pplan);

  // Read config from Physical Plan and apply rate limit to connection
  void SetRateLimit(const proto::system::PhysicalPlan& _pplan,
                    const std::string& component,
                    Connection* conn) const;

  class InstanceData {
   public:
    explicit InstanceData(proto::system::Instance* _instance)
        : instance_(_instance), local_spout_(false), conn_(NULL) {}
    ~InstanceData() { delete instance_; }

    void set_local_spout() { local_spout_ = true; }
    bool is_local_spout() { return local_spout_; }
    void set_connection(Connection* _conn) { conn_ = _conn; }

    proto::system::Instance* instance_;
    bool local_spout_;
    Connection* conn_;
  };

  // map from Connection to their task_id
  typedef std::unordered_map<Connection*, sp_int32> ConnectionTaskIdMap;
  ConnectionTaskIdMap active_instances_;
  // map of task id to InstanceData
  typedef std::unordered_map<sp_int32, InstanceData*> TaskIdInstanceDataMap;
  TaskIdInstanceDataMap instance_info_;

  // map of Instance_id to metric
  // Used for back pressure metrics
  typedef unordered_map<sp_string, shared_ptr<heron::common::TimeSpentMetric>> InstanceMetricMap;
  InstanceMetricMap instance_metric_map_;

  // map of Instance_id to queue metric
  typedef unordered_map<sp_string,
                        shared_ptr<heron::common::MultiMeanMetric>> ConnectionBufferMetricMap;
  ConnectionBufferMetricMap connection_buffer_metric_map_;

  // map of Instance_id to queue length metric
  typedef std::unordered_map<sp_string, shared_ptr<heron::common::MultiCountMetric>>
    ConnectionBufferLengthMetricMap;
  ConnectionBufferLengthMetricMap connection_buffer_length_metric_map_;

  // map of task id to task name
  std::unordered_map<sp_int32, sp_string> task_id_to_name;

  // instances/ causing back pressure
  std::unordered_set<sp_string> remote_ends_who_caused_back_pressure_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  std::vector<sp_string> expected_instances_;
  StMgr* stmgr_;

  // Metrics
  shared_ptr<heron::common::MetricsMgrSt> metrics_manager_client_;
  shared_ptr<heron::common::MultiCountMetric> instance_server_metrics_;
  shared_ptr<heron::common::TimeSpentMetric> back_pressure_metric_aggr_;
  shared_ptr<heron::common::TimeSpentMetric> back_pressure_metric_caused_by_local_instances_;

  bool spouts_under_back_pressure_;

  // Stateful processing related member variables
  shared_ptr<NeighbourCalculator> neighbour_calculator_;
  shared_ptr<CheckpointGateway> stateful_gateway_;

  bool droptuples_upon_backpressure_;

  sp_string heron_tuple_set_2_ = "heron.proto.system.HeronTupleSet2";
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_INSTANCE_SERVER_H_
