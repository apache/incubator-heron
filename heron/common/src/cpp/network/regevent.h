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
// Defines the SS_RegisteredEvent that is used by the EventLoopImpl
///////////////////////////////////////////////////////////////////////////////

#ifndef SELECTSERVER_INTERNAL_H_
#define SELECTSERVER_INTERNAL_H_

#include <event.h>
#include <sys/time.h>
#include <functional>
#include "basics/sptypes.h"
#include "basics/spconsts.h"
#include "network/event_loop.h"

#define MICROSECS_PER_SEC (1000000)

/*
 * SS_RegisteredEvent is nothing but a structure holding all associated information with the
 * registration of a particular read/write/timer event
 */
template <typename T>
class SS_RegisteredEvent {
 public:
  SS_RegisteredEvent(const T& fd, bool persistent, VCallback<EventLoop::Status> cb, sp_int64 mSecs)
      : fd_(fd), persistent_(persistent), cb_(std::move(cb)) {
    timer_.tv_sec = mSecs / MICROSECS_PER_SEC;
    timer_.tv_usec = mSecs % MICROSECS_PER_SEC;
  }

  ~SS_RegisteredEvent() {}

  // Simple accessor functions
  struct event* event() {
    return &ev_;
  }
  struct timeval* timer() {
    return &timer_;
  }
  T get_fd() const { return fd_; }
  // TODO(kramasamy): Returning reference to a member variable is not a good practice.
  // Need to change this to a 'run' method instead of returing the callback
  const VCallback<EventLoop::Status>& get_callback() const { return cb_; }
  bool isPersistent() { return persistent_; }

 private:
  // The underlying libevent event associated
  struct event ev_;
  // Either the file descriptor in read/write cases, or the timerid in timer cases.
  T fd_;
  // Whether this is a persistent register
  bool persistent_;
  // What callback to call
  VCallback<EventLoop::Status> cb_;
  // The timer value associated with this registration.
  struct timeval timer_;
};

#endif  // SELECTSERVER_INTERNAL_H_
