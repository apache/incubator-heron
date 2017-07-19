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

#ifndef __DUMMYTMASTER_H_
#define __DUMMYTMASTER_H_

#include <map>
#include <string>
#include <vector>
#include "network/network_error.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "manager/tmaster.h"

namespace heron {
namespace testing {

class DummyTMaster : public Server {
 public:
  DummyTMaster(EventLoop* eventLoop, const NetworkOptions& options);
  ~DummyTMaster();

  const tmaster::StMgrMap& stmgrs() const { return stmgrs_; }

 protected:
  virtual void HandleNewConnection(Connection* _conn);
  virtual void HandleConnectionClose(Connection* _conn, NetworkErrorCode _status);

 private:
  void HandleRegisterRequest(REQID _id, Connection* _conn,
                             proto::tmaster::StMgrRegisterRequest* _request);
  void HandleHeartbeatRequest(REQID _id, Connection* _conn,
                              proto::tmaster::StMgrHeartbeatRequest* _request);
  tmaster::StMgrMap stmgrs_;
};
}  // namespace testing
}  // namespace heron

#endif
