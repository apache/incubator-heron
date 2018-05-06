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

#ifndef HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_IMPL_H_
#define HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_IMPL_H_

#include <functional>
#include <unordered_map>
#include <list>
#include "basics/basics.h"
#include "network/event_loop.h"

// Forward declarations
struct event_base;
template <typename T>
class SS_RegisteredEvent;

/*
 * A libevent based single-threaded implementation of EventLoop
 * NOTE: Not thread-safe
 */
class EventLoopImpl : public EventLoop {
 public:
  // Constructor/Destructor
  EventLoopImpl();
  virtual ~EventLoopImpl();

  // Methods inherited from EventLoop.
  virtual void loop();
  virtual sp_int32 loopExit();
  virtual sp_int32 registerForRead(sp_int32 fd, VCallback<EventLoop::Status> cb, bool persistent,
                                   sp_int64 timeoutMicroSecs);
  virtual sp_int32 registerForRead(sp_int32 fd, VCallback<EventLoop::Status> cb, bool persistent);
  virtual sp_int32 unRegisterForRead(sp_int32 fd);
  virtual sp_int32 registerForWrite(sp_int32 fd, VCallback<EventLoop::Status> cb, bool persistent,
                                    sp_int64 timeoutMicroSecs);
  virtual sp_int32 registerForWrite(sp_int32 fd, VCallback<EventLoop::Status> cb, bool persistent);
  virtual sp_int32 unRegisterForWrite(sp_int32 fd);
  virtual sp_int64 registerTimer(VCallback<EventLoop::Status> cb, bool persistent,
                                 sp_int64 tMicroSecs);
  virtual sp_int32 unRegisterTimer(sp_int64 timerid);
  virtual void registerInstantCallback(VCallback<> cb);
  struct event_base* dispatcher() {
    return mDispatcher;
  }

  // Static member functions to interact with C libevent API
  static void eventLoopImplReadCallback(sp_int32 fd, sp_int16 event, void* arg);
  static void eventLoopImplWriteCallback(sp_int32 fd, sp_int16 event, void* arg);
  static void eventLoopImplTimerCallback(sp_int32, sp_int16 event, void* arg);

 private:
  // Utility function that maps libevent's status codes to EventLoopImpl's status codes
  EventLoopImpl::Status mapStatusCode(sp_int16 event);

  // Handler function for dispatching instant callbacks
  void handleInstantCallback(Status status);

  // libevent callback on read events.
  void handleReadCallback(sp_int32 fd, sp_int16 event);

  // libevent callback on write events.
  void handleWriteCallback(sp_int32 fd, sp_int16 event);

  // libevent callback on timer events.
  void handleTimerCallback(sp_int16 event, sp_int64 timerid);

  // The underlying dispatcher that we wrap around.
  struct event_base* mDispatcher;

  // The registered read fds.
  std::unordered_map<sp_int32, SS_RegisteredEvent<sp_int32>*> mReadEvents;

  // The registered write fds.
  std::unordered_map<sp_int32, SS_RegisteredEvent<sp_int32>*> mWriteEvents;

  // The registered timers.
  std::unordered_map<sp_int64, SS_RegisteredEvent<sp_int64>*> mTimerEvents;

  // The registered instant callbacks
  typedef std::list<VCallback<>> OrderedCallbackList;
  OrderedCallbackList mListInstantCallbacks;
  // This is the id of the single zero timer that get's registered for all
  // the instant callbacks
  sp_int64 mInstantZeroTimerId;

  // a counter to generate unique timer ids
  sp_int64 mTimerId;
  sp_int64 getNextTimerId() { return mTimerId++; }
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_IMPL_H_
