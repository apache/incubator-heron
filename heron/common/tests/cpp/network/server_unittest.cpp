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

#include <iostream>
#include <vector>
#include <algorithm>
#include "network/server_unittest.h"
#include "network/unittests.pb.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

TestServer::TestServer(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options)
    : Server(eventLoop, _options) {
  InstallMessageHandler(&TestServer::HandleTestMessage);
  InstallMessageHandler(&TestServer::HandleTerminateMessage);
  nrecv_ = nsent_ = 0;
}

TestServer::~TestServer() {}

void TestServer::HandleNewConnection(Connection* _conn) {
  if (clients_.find(_conn) != clients_.end()) return;

  clients_.insert(_conn);
  vclients_.push_back(_conn);
}

void TestServer::HandleConnectionClose(Connection* _conn,
                                       NetworkErrorCode _status __attribute__((unused))) {
  if (clients_.find(_conn) == clients_.end()) return;

  clients_.erase(_conn);

  auto it = std::remove(vclients_.begin(), vclients_.end(), _conn);

  vclients_.erase(it, vclients_.end());
}

void TestServer::HandleTestMessage(Connection* _connection __attribute__((unused)),
                                   pool_unique_ptr<TestMessage> _message) {
  nrecv_++;

  // find a random client to send the message to
  sp_int32 r = rand() % vclients_.size();

  Connection* sc = vclients_[r];
  SendMessage(sc, *_message);

  nsent_++;
}

void TestServer::Terminate() {
  Stop();
  getEventLoop()->loopExit();
}

void TestServer::HandleTerminateMessage(Connection* _connection __attribute__((unused)),
                              pool_unique_ptr<TerminateMessage> _message __attribute__((unused))) {
  AddTimer([this]() { this->Terminate(); }, 1);
}
