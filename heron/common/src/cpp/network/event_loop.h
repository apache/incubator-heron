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

#ifndef HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_H_
#define HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_H_

#include <functional>
#include <unordered_map>
#include "basics/basics.h"

// Forward declarations
struct event_base;

// Represents a callback that returns void but takes any number of
// input arguments.
template <typename... Args>
using VCallback = std::function<void(Args...)>;

/**
 * EventLoop defines a interface to perform non-blocking I/O operations and
 * schedule tasks asynchronously.

 * Programs can register a file descriptor to be notified on a I/O (read or write event)
 * on the descriptor. The notification is communicated through a callback that was registered
 * along with the descriptor.

 * Programs can also register tasks to be run after a certain point in time and be
 * configured to run periodically.
 */
class EventLoop {
 public:
  // User callbacks are supplied with a status upon invokation.
  enum Status {
    // Uknown event happened. This in theory should not happen
    UNKNOWN_EVENT = 0,
    // A file descriptor is ready for read
    READ_EVENT,
    // A file descriptor is ready for write
    WRITE_EVENT,
    // A signal occured when doing an io operation
    SIGNAL_EVENT,
    // A timeout occured
    TIMEOUT_EVENT,
    // A system error occured
    SYSTEM_ERROR_EVENT,
    NUM_EVENT_TYPES
  };

  EventLoop() {}
  virtual ~EventLoop() {}

  /**
   * Start listening for events on registered file descriptors or tasks.
   * Upon an event, the loop executes the corresponding callback.
   * The loop never exits until someone call loopExit.
   */
  virtual void loop() = 0;

  /**
   * Exit the loop. The method returns immediately but the loop exits
   * after it has processed the current event.
   */
  virtual sp_int32 loopExit() = 0;

  /**
   * Register a callback `cb` to be called when the given file descriptor `fd`
   * is ready for reading. Upon receiving a event on the fd,
   * callback will be called with a status of 'READ_EVENT'.

   * If `persistent` is set to 'false', we only monitor it the first time.
   * Else, this `fd` is monitored until 'unRegisterForRead' is called.

   * `timeoutMicroSecs` sets a timeout after which the callback will
   * be called with a status of 'TIMEOUT_EVENT'.
   * `timeoutMicroSecs` <= 0, implies no timeout.

   * A return value of:
   * - 0 indicates successful registration.
   * - negative indicates that the registration failed
   * TODO(Vikasr): Define error return codes for different cases.
   */
  virtual sp_int32 registerForRead(sp_int32 fd, VCallback<Status> cb, bool persistent,
                                   sp_int64 timeoutMicroSecs) = 0;

  // This is the same as registerForRead(fd, cb, persistent, -1)
  virtual sp_int32 registerForRead(sp_int32 fd, VCallback<Status> cb, bool persistent) = 0;

  /**
   * Unregisters a previously registered file descriptor fd for reading.
   * After this call, the read events are no longer delivered for this fd.

   * A return value of :
   * - 0 indicates successful unregistration
   * - negative indicates that this fd wans't registered at all.
   */
  virtual sp_int32 unRegisterForRead(sp_int32 fd) = 0;

  /**
   * Register a callback `cb` to be called when the given file descriptor `fd`
   * is ready for writing. Upon receiving a event on the fd,
   * callback will be called with a status of 'WRITE_EVENT'.

   * If `persistent` is set to 'false', we only monitor it the first time.
   * Else, this `fd` is monitored until 'unRegisterForWrite' is called.

   * `timeoutMicroSecs` sets a timeout after which the callback will
   * be called with a status of 'TIMEOUT_EVENT'.
   * `timeoutMicroSecs` <= 0, implies no timeout.

   * A return value of:
   * - 0 indicates successful registration.
   * - negative indicates that the registration failed
   * TODO(Vikasr): Define error return codes for different cases.
   */
  virtual sp_int32 registerForWrite(sp_int32 fd, VCallback<Status> cb, bool persistent,
                                    sp_int64 timeoutMicroSecs) = 0;

  // This is the same as registerForWrite(fd, cb, persistent, -1)
  virtual sp_int32 registerForWrite(sp_int32 fd, VCallback<Status> cb, bool persistent) = 0;

  /**
   * Unregisters a previously registered file descriptor fd for writing.
   * After this call, the write events are no longer delivered for this fd.

   * A return value of :
   * - 0 indicates successful unregistration
   * - negative indicates that this fd wans't registered at all.
   */
  virtual sp_int32 unRegisterForWrite(sp_int32 fd) = 0;

  /**
   * Register the callback `cb` to be called after `tMicroSecs` micro seconds.
   * If `persistent` is set to 'true', the timer is reinstated to be called
   * after `tMicroSecs` after the invokation of the previous one.

   * The callback will be called with a 'TIMEOUT_EVENT' status.
   * TODO (vikasr): What if re-instating timer fails?

   * Returns a unique id associated with this timer, that can be later
   * used to unregister.
   */
  virtual sp_int64 registerTimer(VCallback<Status> cb, bool persistent, sp_int64 tMicroSecs) = 0;

  /**
   * Unregisters a previously registered timer id.
   * After this call, the events associated with this timer will no
   * longer be delivered.
   */
  virtual sp_int32 unRegisterTimer(sp_int64 timerid) = 0;

  /**
   * Register apis for registering callbacks associated with a zero timer.
   * Such callbacks are registered so that they get called with a small stack
   * footprint (stack unwinding). Multiple callbacks can get registered
   * for one expiry of a 0 timer.
   * The callbacks are guaranteed to be called in the order of their registration.
   * The callbacks are called on the next return to the the libevent loop.
   * TODO (vikasr): Figure out how this is different from registerTimer(cb, false, 0)
   */

  virtual void registerInstantCallback(VCallback<> cb) = 0;

  // TODO(vikasr): event_base shouldn't be used here directly.
  virtual struct event_base* dispatcher() = 0;
};

#endif  // HERON_COMMON_SRC_CPP_NETWORK_EVENT_LOOP_H_
