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

#ifndef __TEST_CLIENT_H
#define __TEST_CLIENT_H

#include <functional>
#include "network/unittests.pb.h"
#include "network/network_error.h"
#include "network/network.h"
#include "basics/basics.h"

class TestClient : public Client {
 public:
  TestClient(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options,
          sp_uint64 _ntotal);

  ~TestClient();

 protected:
  void Retry() { Start(); }

  // Handle incoming connections
  virtual void HandleConnect(NetworkErrorCode _status);

  // Handle connection close
  virtual void HandleClose(NetworkErrorCode _status);

 private:
  // Handle incoming message
  void HandleTestMessage(pool_unique_ptr<TestMessage> _message);

  void SendMessages();
  void CreateAndSendMessage();

  VCallback<> retry_cb_;

  time_t start_time_;

  sp_uint64 nsent_;
  sp_uint64 nrecv_;
  sp_uint64 ntotal_;
};

#endif
