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

#ifndef __TESTSERVER_H
#define __TESTSERVER_H

#include <set>
#include <vector>
#include "network/unittests.pb.h"
#include "network/network_error.h"
#include "network/network.h"
#include "basics/basics.h"

class TestServer : public Server {
 public:
  TestServer(std::shared_ptr<EventLoopImpl> ss, const NetworkOptions& options);

  ~TestServer();

  // total packets recvd
  sp_uint64 recv_pkts() { return nrecv_; }

  // total packets sent
  sp_uint64 sent_pkts() { return nsent_; }

 protected:
  // handle an incoming connection from server
  virtual void HandleNewConnection(Connection* newConnection);

  // handle a connection close
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  // handle the test message
  virtual void HandleTestMessage(Connection* connection, pool_unique_ptr<TestMessage> message);

  // handle the terminate message
  virtual void HandleTerminateMessage(Connection* connection,
                                      pool_unique_ptr<TerminateMessage> message);

 private:
  void Terminate();

  std::set<Connection*> clients_;
  std::vector<Connection*> vclients_;

  sp_uint64 nrecv_;
  sp_uint64 nsent_;
};

#endif
