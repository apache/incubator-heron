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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_

#include <unordered_map>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class MetricsMgrSt;
class MultiCountMetric;
}
}

namespace heron {
namespace stmgr {

using std::shared_ptr;

class StMgr;
class StMgrClient;

class StMgrClientMgr {
 public:
  StMgrClientMgr(shared_ptr<EventLoop> eventLoop, const sp_string& _topology_name,
                 const sp_string& _topology_id, const sp_string& _stmgr_id, StMgr* _stream_manager,
                 shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client,
                 sp_int64 _high_watermark, sp_int64 _low_watermark,
                 bool _droptuples_upon_backpressure);
  virtual ~StMgrClientMgr();

  // Start the appropriate clients based on a new physical plan
  virtual void StartConnections(proto::system::PhysicalPlan const& _pplan);
  // return true if we are successful in sending the message. false otherwise
  bool SendTupleStreamMessage(sp_int32 _task_id,
                              const sp_string& _stmgr_id,
                              const proto::system::HeronTupleSet2& _msg);

  // Forward the call to the stmgr
  void StartBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Forward the call to the stmgr
  void StopBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Used by the server to tell the client to send the back pressure related
  // messages
  void SendStartBackPressureToOtherStMgrs();
  void SendStopBackPressureToOtherStMgrs();
  bool DidAnnounceBackPressure();
  // Called by StMgrClient when its connection closes
  void HandleDeadStMgrConnection(const sp_string& _stmgr_id);
  // Called by StMgrClient when it successfully registers
  void HandleStMgrClientRegistered();
  void SendDownstreamStatefulCheckpoint(const sp_string& _stmgr_id,
                                        proto::ckptmgr::DownstreamStatefulCheckpoint* _message);

  // Interface to close all connections
  virtual void CloseConnectionsAndClear();

  // Check if all clients are registered
  virtual bool AllStMgrClientsRegistered();

 private:
  StMgrClient* CreateClient(const sp_string& _other_stmgr_id,
                            const sp_string& _host_name, sp_int32 _port);

  // map of stmgrid to its client
  std::unordered_map<sp_string, StMgrClient*> clients_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  shared_ptr<EventLoop> eventLoop_;

  StMgr* stream_manager_;
  // Metrics
  shared_ptr<heron::common::MetricsMgrSt> metrics_manager_client_;
  shared_ptr<heron::common::MultiCountMetric> stmgr_clientmgr_metrics_;

  sp_int64 high_watermark_;
  sp_int64 low_watermark_;

  bool droptuples_upon_backpressure_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_
