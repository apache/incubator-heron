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

#include "network/oclient_unittest.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include "network/unittests.pb.h"
#include "network/host_unittest.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

OrderClient::OrderClient(EventLoopImpl* eventLoop, const NetworkOptions& _options,
                         sp_uint64 _ntotal)
    : Client(eventLoop, _options), ntotal_(_ntotal) {
  InstallMessageHandler(&OrderClient::HandleOrderMessage);
  start_time_ = time(NULL);
  nsent_ = nrecv_ = msgids_ = msgidr_ = 0;

  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this]() { this->Retry(); };
}

void OrderClient::CreateAndSendMessage() {
  OrderMessage* message = new OrderMessage();

  message->set_id(msgids_++);

  SendMessage(message);
  return;
}

void OrderClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    SendMessages();
  } else {
    // Retry after some time
    AddTimer(retry_cb_, RETRY_TIMEOUT);
  }
}

void OrderClient::HandleClose(NetworkErrorCode) {}

void OrderClient::HandleOrderMessage(OrderMessage* _message) {
  ++nrecv_;

  EXPECT_EQ(msgidr_++, _message->id());

  delete _message;

  if (nrecv_ >= ntotal_) {
    Stop();
    getEventLoop()->loopExit();
  }
}

void OrderClient::SendMessages() {
  while (getOutstandingPackets() < 10000) {
    CreateAndSendMessage();
    if (++nsent_ >= ntotal_) {
      return;
    }
  }

  // every ms - call send messages
  AddTimer([this]() { this->SendMessages(); }, 1000);
}
