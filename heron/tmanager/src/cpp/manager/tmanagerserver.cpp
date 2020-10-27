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

#include "manager/tmanagerserver.h"
#include <iostream>
#include "manager/tmetrics-collector.h"
#include "manager/tmanager.h"
#include "processor/processor.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "metrics/tmanager-metrics.h"

namespace heron {
namespace tmanager {

using std::unique_ptr;
using std::shared_ptr;

TManagerServer::TManagerServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                             shared_ptr<TMetricsCollector> _collector, TManager* _tmanager)
    : Server(eventLoop, _options), collector_(_collector), tmanager_(_tmanager) {
  // Install the stmgr handlers
  InstallRequestHandler(&TManagerServer::HandleStMgrRegisterRequest);
  InstallRequestHandler(&TManagerServer::HandleStMgrHeartbeatRequest);
  InstallMessageHandler(&TManagerServer::HandleInstanceStateStored);
  InstallMessageHandler(&TManagerServer::HandleRestoreTopologyStateResponse);
  InstallMessageHandler(&TManagerServer::HandleResetTopologyStateMessage);

  // Install the metricsmgr handlers
  InstallMessageHandler(&TManagerServer::HandleMetricsMgrStats);
}

TManagerServer::~TManagerServer() {
  // Nothing really
}

void TManagerServer::HandleNewConnection(Connection* conn) {
  // There is nothing to be done here. Instead we wait for
  // the register message
}

void TManagerServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  if (tmanager_->RemoveStMgrConnection(_conn) != proto::system::OK) {
    LOG(WARNING) << "Unknown connection closed on us from " << _conn->getIPAddress() << ":"
                 << _conn->getPort() << ", possibly metrics mgr";
    return;
  }
}

void TManagerServer::HandleStMgrRegisterRequest(REQID _reqid, Connection* _conn,
                                   pool_unique_ptr<proto::tmanager::StMgrRegisterRequest> _request) {
  unique_ptr<StMgrRegisterProcessor> processor =
      make_unique<StMgrRegisterProcessor>(_reqid, _conn, std::move(_request), tmanager_, this);
  processor->Start();
}

void TManagerServer::HandleStMgrHeartbeatRequest(REQID _reqid, Connection* _conn,
                                  pool_unique_ptr<proto::tmanager::StMgrHeartbeatRequest> _request) {
  unique_ptr<StMgrHeartbeatProcessor> processor =
      make_unique<StMgrHeartbeatProcessor>(_reqid, _conn, std::move(_request), tmanager_, this);
  processor->Start();
}

void TManagerServer::HandleMetricsMgrStats(Connection*,
                                        pool_unique_ptr<proto::tmanager::PublishMetrics> _request) {
  collector_->AddMetric(*_request);
}

void TManagerServer::HandleInstanceStateStored(Connection*,
                                    pool_unique_ptr<proto::ckptmgr::InstanceStateStored> _message) {
  tmanager_->HandleInstanceStateStored(_message->checkpoint_id(), _message->instance());
}

void TManagerServer::HandleRestoreTopologyStateResponse(Connection* _conn,
                           pool_unique_ptr<proto::ckptmgr::RestoreTopologyStateResponse> _message) {
  tmanager_->HandleRestoreTopologyStateResponse(_conn, _message->checkpoint_id(),
                                               _message->restore_txid(),
                                               _message->status().status());
}

void TManagerServer::HandleResetTopologyStateMessage(Connection* _conn,
                                    pool_unique_ptr<proto::ckptmgr::ResetTopologyState> _message) {
  tmanager_->ResetTopologyState(_conn, _message->dead_stmgr(),
                               _message->dead_taskid(), _message->reason());
}
}  // namespace tmanager
}  // namespace heron
