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

#ifndef __TMASTERSERVER_H
#define __TMASTERSERVER_H

#include "network/network_error.h"
#include "network/network.h"
#include "proto/tmaster.pb.h"
#include "proto/ckptmgr.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmaster {

class TMaster;
class TMetricsCollector;

class TMasterServer : public Server {
 public:
  TMasterServer(EventLoop* eventLoop, const NetworkOptions& options, TMetricsCollector* _collector,
                TMaster* _tmaster);
  virtual ~TMasterServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests
  void HandleStMgrRegisterRequest(REQID _id, Connection* _conn,
                                  proto::tmaster::StMgrRegisterRequest* _request);
  void HandleStMgrHeartbeatRequest(REQID _id, Connection* _conn,
                                   proto::tmaster::StMgrHeartbeatRequest* _request);
  void HandleMetricsMgrStats(Connection*, proto::tmaster::PublishMetrics* _request);

  // Message sent by stmgr to tell tmaster that a particular checkpoint message
  // was saved. This way the tmaster can keep track of which all instances have saved their
  // state for any given checkpoint id.
  void HandleInstanceStateStored(Connection*, proto::ckptmgr::InstanceStateStored* _message);
  // Handle response from stmgr for the RestoreTopologyStateRequest
  void HandleRestoreTopologyStateResponse(Connection*,
                                     proto::ckptmgr::RestoreTopologyStateResponse* _message);
  // Stmgr can request tmaster to reset the state of the topology in case it finds any errors.
  void HandleResetTopologyStateMessage(Connection*,
                                     proto::ckptmgr::ResetTopologyState* _message);

  // our tmaster
  TMetricsCollector* collector_;
  TMaster* tmaster_;
};
}  // namespace tmaster
}  // namespace heron

#endif
