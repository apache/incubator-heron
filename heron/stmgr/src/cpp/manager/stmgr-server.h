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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_

#include <map>
#include <set>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class MetricsMgrSt;
class MultiCountMetric;
class TimeSpentMetric;
class AssignableMetric;
}
}

namespace heron {
namespace stmgr {

class StMgr;

class StMgrServer : public Server {
 public:
  StMgrServer(EventLoop* eventLoop, const NetworkOptions& options, const sp_string& _topology_name,
              const sp_string& _topology_id, const sp_string& _stmgr_id,
              const std::vector<sp_string>& _expected_instances, StMgr* _stmgr,
              heron::common::MetricsMgrSt* _metrics_manager_client);
  virtual ~StMgrServer();

  void SendToInstance2(sp_int32 _task_id, const proto::system::HeronTupleSet2& _message);
  void SendToInstance2(sp_int32 _task_id,
                       sp_int32 _byte_size,
                       const sp_string _type_name,
                       const char* _message);

  void BroadcastNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  // Do back pressure
  void StartBackPressureClientCb(const sp_string& _other_stmgr_id);
  // Relieve back pressure
  void StopBackPressureClientCb(const sp_string& _other_stmgr_id);

  bool HaveAllInstancesConnectedToUs() const {
    return active_instances_.size() == expected_instances_.size();
  }

  // Gets all the Instance information
  void GetInstanceInfo(std::vector<proto::system::Instance*>& _return);

  bool DidAnnounceBackPressure() { return !remote_ends_who_caused_back_pressure_.empty(); }

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  sp_string MakeBackPressureCompIdMetricName(const sp_string& instanceid);
  sp_string MakeQueueSizeCompIdMetricName(const sp_string& instanceid);
  sp_string GetInstanceName(Connection* _connection);

  // Various handlers for different requests

  // First from other stream managers
  void HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                               proto::stmgr::StrMgrHelloRequest* _request);
  void HandleTupleStreamMessage(Connection* _conn, proto::stmgr::TupleStreamMessage2* _message);

  // Next from local instances
  void HandleRegisterInstanceRequest(REQID _id, Connection* _conn,
                                     proto::stmgr::RegisterInstanceRequest* _request);
  void HandleTupleSetMessage(Connection* _conn, proto::system::HeronTupleSet* _message);

  // Backpressure message from and to other stream managers
  void HandleStartBackPressureMessage(Connection* _conn,
                                      proto::stmgr::StartBackPressureMessage* _message);
  void HandleStopBackPressureMessage(Connection* _conn,
                                     proto::stmgr::StopBackPressureMessage* _message);
  void SendStartBackPressureToOtherStMgrs();
  void SendStopBackPressureToOtherStMgrs();

  // Back pressure related connection callbacks
  // Do back pressure
  void StartBackPressureConnectionCb(Connection* _connection);
  // Relieve back pressure
  void StopBackPressureConnectionCb(Connection* _connection);

  // Connection buffer size metric
  void ConnectionBufferChangeCb(Connection* _connection);

  // Can we free the back pressure on the spouts?
  void AttemptStopBackPressureFromSpouts();
  // Start back pressure on the spouts
  void StartBackPressureOnSpouts();

  // Compute the LocalSpouts from Physical Plan
  void ComputeLocalSpouts(const proto::system::PhysicalPlan& _pplan);

  class InstanceData {
   public:
    explicit InstanceData(proto::system::Instance* _instance)
        : instance_(_instance), local_spout_(false), conn_(NULL) {}
    ~InstanceData() { delete instance_; }

    void set_local_spout() { local_spout_ = true; }
    void set_connection(Connection* _conn) { conn_ = _conn; }

    proto::system::Instance* instance_;
    bool local_spout_;
    Connection* conn_;
  };

  // map from stmgr_id to their connection
  typedef std::map<sp_string, Connection*> StreamManagerConnectionMap;
  StreamManagerConnectionMap stmgrs_;
  // Same as above but reverse
  typedef std::map<Connection*, sp_string> ConnectionStreamManagerMap;
  ConnectionStreamManagerMap rstmgrs_;

  // map from Connection to their task_id
  typedef std::map<Connection*, sp_int32> ConnectionTaskIdMap;
  ConnectionTaskIdMap active_instances_;
  // map of task id to InstanceData
  // Once populated, will not change
  typedef std::map<sp_int32, InstanceData*> TaskIdInstanceDataMap;
  TaskIdInstanceDataMap instance_info_;

  // map of Instance_id/stmgrid to metric
  // Used for back pressure metrics
  typedef std::map<sp_string, heron::common::TimeSpentMetric*> InstanceMetricMap;
  InstanceMetricMap instance_metric_map_;

  // map of Instance_id/stmgrid to queue metric
  typedef std::map<sp_string, heron::common::AssignableMetric*> QueueMetricMap;
  QueueMetricMap queue_metric_map_;

  // instances/stream mgrs causing back pressure
  std::set<sp_string> remote_ends_who_caused_back_pressure_;
  // stream managers that have announced back pressure
  std::set<sp_string> stmgrs_who_announced_back_pressure_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  std::vector<sp_string> expected_instances_;
  StMgr* stmgr_;

  // Metrics
  heron::common::MetricsMgrSt* metrics_manager_client_;
  heron::common::MultiCountMetric* stmgr_server_metrics_;
  heron::common::TimeSpentMetric* back_pressure_metric_aggr_;
  heron::common::TimeSpentMetric* back_pressure_metric_initiated_;

  bool spouts_under_back_pressure_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_
