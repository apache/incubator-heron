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

///////////////////////////////////////////////////////////////////////////////
//
// This file defines the NotifyingNotifyingCommunicator class.
// NotifyingNotifyingCommunicator is a mechanism used to communicate objects across
// different eventLoops/threads. Consumers setup an eventLoop and a
// consuming function while they setup the piper. Producers can enqueue
// items from any thread that will then magically be consumed. Producers will get
// notified on their eventLoop of the consumption
//
///////////////////////////////////////////////////////////////////////////////
#ifndef HERON_INSTANCE_SRC_CPP_UTILS_NOTIFYINGCOMMUNICATOR_H_
#define HERON_INSTANCE_SRC_CPP_UTILS_NOTIFYINGCOMMUNICATOR_H_

#include <fcntl.h>
#include <errno.h>

#include "basics/basics.h"
#include "threads/threads.h"
#include "network/event_loop.h"

#include "utils/communicator.h"

namespace heron {
namespace instance {

/*
 * Piper class definition
 */
template<typename T>
class NotifyingCommunicator {
 public:
  // Constructor/Destructor
  NotifyingCommunicator(std::shared_ptr<EventLoop> consumer_loop,
                        std::function<void(T)> consumer_function,
                        std::shared_ptr<EventLoop> notification_loop,
                        std::function<void()> notification_function) {
    consumption_function_ = std::move(consumer_function);
    notification_function_ = std::move(notification_function);
    forward_channel_ = new Communicator<T>(consumer_loop,
                       std::bind(&NotifyingCommunicator::consumption_function, this,
                                 std::placeholders::_1));
    notification_channel_ = new Communicator<char*>(notification_loop,
                              std::bind(&NotifyingCommunicator::notification_channel_consumer,
                                        this, std::placeholders::_1));
  }

  virtual ~NotifyingCommunicator() {
    delete forward_channel_;
    delete notification_channel_;
  }

  void enqueue(T t) {
    forward_channel_->enqueue(std::move(t));
  }

  int size() {
    return forward_channel_->size();
  }

  void stopConsumption() {
    forward_channel_->unRegisterForRead();
  }
  void resumeConsumption() {
    forward_channel_->registerForRead();
  }

 private:
  void notification_channel_consumer(char* c) {
    notification_function_();
  }
  void consumption_function(T t) {
    consumption_function_(std::move(t));
    notification_channel_->enqueue(&unused_);
  }

  Communicator<T>* forward_channel_;
  Communicator<char*>* notification_channel_;
  std::function<void(T)> consumption_function_;
  std::function<void()> notification_function_;
  char unused_;
};

}  // end namespace instance
}  // end namespace heron

#endif  // HERON_INSTANCE_SRC_CPP_UTILS_NOTIFYINGCOMMUNICATOR_H_
