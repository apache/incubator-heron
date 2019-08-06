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

#include "network/oserver_unittest.h"
#include <chrono>
#include <map>
#include <iostream>
#include "network/unittests.pb.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

OrderServer::OrderServer(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options)
    : Server(eventLoop, _options) {
  InstallMessageHandler(&OrderServer::HandleOrderMessage);
  InstallMessageHandler(&OrderServer::HandleTerminateMessage);
  nrecv_ = nsent_ = 0;
}

OrderServer::~OrderServer() {}

void OrderServer::HandleNewConnection(Connection* _conn) {
  if (clients_.find(_conn) != clients_.end()) return;

  clients_[_conn] = new msgid;
}

void OrderServer::HandleConnectionClose(Connection* _conn,
                                        NetworkErrorCode _status __attribute__((unused))) {
  if (clients_.find(_conn) == clients_.end()) return;

  std::map<Connection*, msgid*>::iterator it = clients_.find(_conn);

  msgid* ids = it->second;

  clients_.erase(_conn);

  delete ids;
}

void OrderServer::HandleOrderMessage(Connection* _conn, pool_unique_ptr<OrderMessage> _message) {
  if (clients_.find(_conn) == clients_.end()) return;

  nrecv_++;

  std::map<Connection*, msgid*>::iterator it = clients_.find(_conn);

  EXPECT_EQ(it->second->incr_idr(), _message->id());

  _message->set_id(it->second->incr_ids());
  SendMessage(_conn, *_message);

  nsent_++;
}

void OrderServer::Terminate() {
  Stop();
  getEventLoop()->loopExit();
}

void OrderServer::HandleTerminateMessage(Connection* _connection __attribute__((unused)),
                               pool_unique_ptr<TerminateMessage> _message __attribute__((unused))) {
  AddTimer([this]() { std::cout << "OrderServer:Terminate"; this->Terminate(); }, 1);
}
