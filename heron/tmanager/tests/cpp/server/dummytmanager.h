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

#ifndef __DUMMYTMANAGER_H_
#define __DUMMYTMANAGER_H_

#include <map>
#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "manager/tmanager.h"

namespace heron {
namespace testing {

class DummyTManager : public Server {
 public:
  DummyTManager(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options);
  ~DummyTManager();

  const tmanager::StMgrMap& stmgrs() const { return stmgrs_; }

 protected:
  virtual void HandleNewConnection(Connection* _conn);
  virtual void HandleConnectionClose(Connection* _conn, NetworkErrorCode _status);

 private:
  void HandleRegisterRequest(REQID _id, Connection* _conn,
                             pool_unique_ptr<proto::tmanager::StMgrRegisterRequest> _request);
  void HandleHeartbeatRequest(REQID _id, Connection* _conn,
                              proto::tmanager::StMgrHeartbeatRequest* _request);
  tmanager::StMgrMap stmgrs_;
};
}  // namespace testing
}  // namespace heron

#endif
