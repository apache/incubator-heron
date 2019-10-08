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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_

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
class CountMetric;
class MetricsMgrSt;
class TimeSpentMetric;
}
}

namespace heron {
namespace stmgr {

using std::shared_ptr;

class StMgr;

class StMgrServer : public Server {
 public:
  StMgrServer(shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
              const sp_string& _topology_name,
              const sp_string& _topology_id, const sp_string& _stmgr_id, StMgr* _stmgr,
              shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client);
  virtual ~StMgrServer();

  // Do back pressure
  void StartBackPressureClientCb(const sp_string& _other_stmgr_id);
  // Relieve back pressure
  void StopBackPressureClientCb(const sp_string& _other_stmgr_id);

  bool DidAnnounceBackPressure() { return !remote_ends_who_caused_back_pressure_.empty(); }
  bool DidOthersAnnounceBackPressure() {
    return !stmgrs_who_announced_back_pressure_.empty();
  }

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests

  // First from other stream managers
  void HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                               pool_unique_ptr<proto::stmgr::StrMgrHelloRequest> _request);
  void HandleTupleStreamMessage(Connection* _conn,
                                pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message);

  // Handler for DownstreamStatefulCheckpoint from a peer stmgr
  void HandleDownstreamStatefulCheckpointMessage(Connection* _conn,
                           pool_unique_ptr<proto::ckptmgr::DownstreamStatefulCheckpoint> _message);

  // Backpressure message from and to other stream managers
  void HandleStartBackPressureMessage(Connection* _conn,
                                 pool_unique_ptr<proto::stmgr::StartBackPressureMessage> _message);
  void HandleStopBackPressureMessage(Connection* _conn,
                                 pool_unique_ptr<proto::stmgr::StopBackPressureMessage> _message);

  // map from stmgr_id to their connection
  typedef std::unordered_map<sp_string, Connection*> StreamManagerConnectionMap;
  StreamManagerConnectionMap stmgrs_;
  // Same as above but reverse
  typedef std::unordered_map<Connection*, sp_string> ConnectionStreamManagerMap;
  ConnectionStreamManagerMap rstmgrs_;

  // stream mgrs causing back pressure
  std::unordered_set<sp_string> remote_ends_who_caused_back_pressure_;
  // stream managers that have announced back pressure
  std::unordered_set<sp_string> stmgrs_who_announced_back_pressure_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  StMgr* stmgr_;

  // Metrics
  shared_ptr<heron::common::MetricsMgrSt> metrics_manager_client_;
  shared_ptr<heron::common::CountMetric> tuples_from_stmgrs_metrics_;
  shared_ptr<heron::common::CountMetric> ack_tuples_from_stmgrs_metrics_;
  shared_ptr<heron::common::CountMetric> fail_tuples_from_stmgrs_metrics_;
  shared_ptr<heron::common::CountMetric> bytes_from_stmgrs_metrics_;
  shared_ptr<heron::common::TimeSpentMetric> back_pressure_metric_caused_by_remote_stmgr_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_SERVER_H_
