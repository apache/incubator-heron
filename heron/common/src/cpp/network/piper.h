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
// This file defines the Piper class.
// Piper is a mechanism used to execute arbitrary callbacks
// inside an EventLoop. This allows internal/external third party
// libraries which could be multithreaded to make sure that
// callbacks are executed inside a Heron main thread.
//
///////////////////////////////////////////////////////////////////////////////
#ifndef PIPER_H_
#define PIPER_H_

#include "basics/basics.h"
#include "threads/threads.h"
#include "network/event_loop.h"

/*
 * Piper class definition
 */
class Piper {
 public:
  // Constructor/Destructor
  explicit Piper(std::shared_ptr<EventLoop> eventLoop);

  virtual ~Piper();

  // The main interface of Piper. This call makes the Callback to
  // be executed in the event loop thread.
  void ExecuteInEventLoop(VCallback<> _cb);

 private:
  // This is the function used to signal the main thread
  void SignalMainThread();

  // This is the function that is registered in the event loop
  // to be called when awoken
  void OnWakeUp(EventLoop::Status status);

  std::shared_ptr<EventLoop> eventLoop_;

  // These pipers are how they communicate it across to our thread
  sp_int32 pipers_[2];
  // This is where callbacks are queued
  PCQueue<CallBack*>* cbs_;
};

#endif  // PIPER_H_
