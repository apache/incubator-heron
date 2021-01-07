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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_

#include "network/network_error.h"
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

class StMgrClientMgr;

class StMgrClient : public Client {
 public:
  StMgrClient(shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
              const sp_string& _topology_name,
              const sp_string& _topology_id, const sp_string& _our_id, const sp_string& _other_id,
              StMgrClientMgr* _client_manager,
              shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client,
              bool _droptuples_upon_backpressure);
  virtual ~StMgrClient();

  void Quit();

  // Return true if successful in sending the message. false otherwise
  bool SendTupleStreamMessage(proto::stmgr::TupleStreamMessage& _msg);
  void SendStartBackPressureMessage();
  void SendStopBackPressureMessage();
  void SendDownstreamStatefulCheckpoint(proto::ckptmgr::DownstreamStatefulCheckpoint* _message);
  bool IsRegistered() const { return is_registered_; }

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleHelloResponse(
          void*,
          pool_unique_ptr<proto::stmgr::StrMgrHelloResponse> _response,
          NetworkErrorCode);
  void HandleTupleStreamMessage(pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message);

  void OnReConnectTimer();
  void SendHelloRequest();
  // Do back pressure
  virtual void StartBackPressureConnectionCb(Connection* _connection);
  // Relieve back pressure
  virtual void StopBackPressureConnectionCb(Connection* _connection);

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string our_stmgr_id_;
  sp_string other_stmgr_id_;
  bool quit_;

  StMgrClientMgr* client_manager_;
  // Metrics
  shared_ptr<heron::common::MetricsMgrSt> metrics_manager_client_;
  shared_ptr<heron::common::MultiCountMetric> stmgr_client_metrics_;

  // Configs to be read
  sp_int32 reconnect_other_streammgrs_interval_sec_;

  // Counters
  sp_int64 ndropped_messages_;
  sp_int32 reconnect_attempts_;

  // Have we registered ourselves
  bool is_registered_;
  // Do we drop tuples upon backpressure
  bool droptuples_upon_backpressure_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENT_H_
