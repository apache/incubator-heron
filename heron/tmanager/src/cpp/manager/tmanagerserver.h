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

#ifndef __TMANAGERSERVER_H
#define __TMANAGERSERVER_H

#include "network/network_error.h"
#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "proto/ckptmgr.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmanager {

using std::shared_ptr;

class TManager;
class TMetricsCollector;

class TManagerServer : public Server {
 public:
  TManagerServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
          shared_ptr<TMetricsCollector> _collector, TManager* _tmanager);
  virtual ~TManagerServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests
  void HandleStMgrRegisterRequest(REQID _id, Connection* _conn,
                                  pool_unique_ptr<proto::tmanager::StMgrRegisterRequest> _request);
  void HandleStMgrHeartbeatRequest(REQID _id, Connection* _conn,
                                   pool_unique_ptr<proto::tmanager::StMgrHeartbeatRequest> _request);
  void HandleMetricsMgrStats(Connection*, pool_unique_ptr<proto::tmanager::PublishMetrics> _request);

  // Message sent by stmgr to tell tmanager that a particular checkpoint message
  // was saved. This way the tmanager can keep track of which all instances have saved their
  // state for any given checkpoint id.
  void HandleInstanceStateStored(Connection*,
                                 pool_unique_ptr<proto::ckptmgr::InstanceStateStored> _message);
  // Handle response from stmgr for the RestoreTopologyStateRequest
  void HandleRestoreTopologyStateResponse(Connection*,
                            pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateResponse> _message);
  // Stmgr can request tmanager to reset the state of the topology in case it finds any errors.
  void HandleResetTopologyStateMessage(Connection*,
                                      pool_unique_ptr<proto::ckptmgr::ResetTopologyState> _message);

  // our tmanager
  shared_ptr<TMetricsCollector> collector_;
  TManager* tmanager_;
};
}  // namespace tmanager
}  // namespace heron

#endif
