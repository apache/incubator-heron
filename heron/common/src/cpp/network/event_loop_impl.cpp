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

#include "network/event_loop_impl.h"
#include <signal.h>
#include <errno.h>
#include <iostream>
#include "glog/logging.h"
#include "basics/basics.h"
#include "errors/spexcept.h"
#include "network/regevent.h"

// 'C' style callback for libevent on read events
void EventLoopImpl::eventLoopImplReadCallback(sp_int32 fd, sp_int16 event, void* arg) {
  auto* el = reinterpret_cast<EventLoopImpl*>(arg);
  el->handleReadCallback(fd, event);
}

// 'C' style callback for libevent on write events
void EventLoopImpl::eventLoopImplWriteCallback(sp_int32 fd, sp_int16 event, void* arg) {
  auto* el = reinterpret_cast<EventLoopImpl*>(arg);
  el->handleWriteCallback(fd, event);
}

// 'C' style callback for libevent on timer events
void EventLoopImpl::eventLoopImplTimerCallback(sp_int32, sp_int16 event, void* arg) {
  // TODO(vikasr): this needs to change to VCallback
  auto* cb = (CallBack1<sp_int16>*)arg;
  cb->Run(event);
}

// Constructor. We create a new event_base.
EventLoopImpl::EventLoopImpl() {
  mTimerId = 1;
  mInstantZeroTimerId = -1;
  mDispatcher = event_base_new();
}

// Destructor. Clear read/write/timer events and then clear the event_base
EventLoopImpl::~EventLoopImpl() {
  for (auto iter = mReadEvents.begin(); iter != mReadEvents.end(); ++iter) {
    event_del(iter->second->event());
    delete iter->second;
  }

  mReadEvents.clear();

  for (auto iter = mWriteEvents.begin(); iter != mWriteEvents.end(); ++iter) {
    event_del(iter->second->event());
    delete iter->second;
  }

  mWriteEvents.clear();

  for (auto iter = mTimerEvents.begin(); iter != mTimerEvents.end(); ++iter) {
    event_del(iter->second->event());
    delete iter->second;
  }

  mTimerEvents.clear();
  event_base_free(mDispatcher);
}

static void handleTerm(evutil_socket_t _, sp_int16 what, void* ctx) {
  struct event_base *evb = (struct event_base*) ctx;
  event_base_loopbreak(evb);
}

void EventLoopImpl::loop() {
  struct event *term = evsignal_new(mDispatcher, SIGTERM, handleTerm, mDispatcher);
  event_add(term, NULL);
  // This never returns
  event_base_dispatch(mDispatcher);
}

int EventLoopImpl::loopExit() { return event_base_loopbreak(mDispatcher); }

int EventLoopImpl::registerForRead(int fd, VCallback<EventLoop::Status> cb, bool persistent) {
  return registerForRead(fd, std::move(cb), persistent, -1);
}

int EventLoopImpl::registerForRead(int fd, VCallback<EventLoop::Status> cb, bool persistent,
                                   sp_int64 mSecs) {
  if (mReadEvents.find(fd) != mReadEvents.end()) {
    // We have already registered this fd for read.
    // Cannot register again.
    return -1;
  }
  // Create the appropriate structures and init them.
  auto* event = new SS_RegisteredEvent<sp_int32>(fd, persistent, std::move(cb), mSecs);
  sp_int16 ev_mask = EV_READ;
  if (persistent) {
    ev_mask |= EV_PERSIST;
  }
  event_set(event->event(), fd, ev_mask, &EventLoopImpl::eventLoopImplReadCallback, this);
  if (event_base_set(mDispatcher, event->event()) < 0) {
    // cout << "event_base_set failed for fd " << fd;
    delete event;
    throw heron::error::Error_Exception(errno);
  }

  // Now add it to the list of fds monitored by the mDispatcher
  if (mSecs < 0) {
    if (event_add(event->event(), NULL) < 0) {
      // cout << "event_add failed for fd " << fd;
      delete event;
      throw heron::error::Error_Exception(errno);
    }
  } else {
    if (event_add(event->event(), event->timer()) < 0) {
      // cout << "event_add failed for fd " << fd;
      delete event;
      throw heron::error::Error_Exception(errno);
    }
  }
  mReadEvents[fd] = event;
  return 0;
}

int EventLoopImpl::unRegisterForRead(int fd) {
  if (mReadEvents.find(fd) == mReadEvents.end()) {
    // This fd wasn't registed at for reading. Hence we can't unregister it.
    return -1;
  }

  // Delete the underlying event in libevent
  if (event_del(mReadEvents[fd]->event()) != 0) {
    // cout << "event_del failed for fd " << fd;
    throw heron::error::Error_Exception(errno);
  }
  delete mReadEvents[fd];
  mReadEvents.erase(fd);
  return 0;
}

int EventLoopImpl::registerForWrite(int fd, VCallback<EventLoop::Status> cb, bool persistent) {
  return registerForWrite(fd, std::move(cb), persistent, -1);
}

int EventLoopImpl::registerForWrite(int fd, VCallback<EventLoop::Status> cb, bool persistent,
                                    sp_int64 mSecs) {
  if (mWriteEvents.find(fd) != mWriteEvents.end()) {
    // We have already registered this fd for write. Cannot register again.
    LOG(ERROR) << "Already registered fd " << fd << ", Cannot register again";
    return -1;
  }

  // Create and init appropriate data structures.
  auto* event = new SS_RegisteredEvent<sp_int32>(fd, persistent, std::move(cb), mSecs);
  sp_int16 ev_mask = EV_WRITE;
  if (persistent) {
    ev_mask |= EV_PERSIST;
  }
  event_set(event->event(), fd, ev_mask, &EventLoopImpl::eventLoopImplWriteCallback, this);
  if (event_base_set(mDispatcher, event->event()) < 0) {
    // cout << "event_base_set failed for fd " << fd;
    delete event;
    throw heron::error::Error_Exception(errno);
  }

  // Now add the fd to libevent to be monitored.
  if (mSecs < 0) {
    if (event_add(event->event(), NULL) < 0) {
      // cout << "event_add failed for fd " << fd;
      delete event;
      throw heron::error::Error_Exception(errno);
    }
  } else {
    if (event_add(event->event(), event->timer()) < 0) {
      // cout << "event_add failed for fd " << fd;
      delete event;
      throw heron::error::Error_Exception(errno);
    }
  }
  mWriteEvents[fd] = event;
  return 0;
}

int EventLoopImpl::unRegisterForWrite(int fd) {
  if (mWriteEvents.find(fd) == mWriteEvents.end()) {
    // This fd wasn't registed at for writing. Hence we can't unregister it.
    return -1;
  }

  // Delete the fd from libevent
  if (event_del(mWriteEvents[fd]->event()) != 0) {
    // cout << "event_del failed for fd " << fd;
    throw heron::error::Error_Exception(errno);
  }
  delete mWriteEvents[fd];
  mWriteEvents.erase(fd);
  return 0;
}

sp_int64 EventLoopImpl::registerTimer(VCallback<EventLoop::Status> cb, bool persistent,
                                      sp_int64 mSecs) {
  // First do some error checking
  if (mSecs < 0) {
    // We cannot register for past can we?
    return -1;
  }

  sp_int64 timerId = getNextTimerId();

  // Create and init the appropriate data structures
  auto* event = new SS_RegisteredEvent<sp_int64>(timerId, persistent, std::move(cb), mSecs);

  CallBack1<sp_int16>* cbS = NULL;
  // TODO(vikasr): this needs to change to VCallback
  if (!persistent) {
    cbS = CreateCallback(this, &EventLoopImpl::handleTimerCallback, timerId);
  } else {
    cbS = CreatePersistentCallback(this, &EventLoopImpl::handleTimerCallback, timerId);
  }

  evtimer_set(event->event(), &EventLoopImpl::eventLoopImplTimerCallback, cbS);
  if (event_base_set(mDispatcher, event->event()) < 0) {
    // cout << "event_base_set failed for timer " << timerId;
    delete event;
    delete cbS;
    throw heron::error::Error_Exception(errno);
  }

  // Now add the timer to libevent
  if (evtimer_add(event->event(), event->timer()) < 0) {
    LOG(ERROR) << "event_add failed for timer " << timerId;
    delete event;
    delete cbS;
    throw heron::error::Error_Exception(errno);
  }
  mTimerEvents[timerId] = event;
  return timerId;
}

sp_int32 EventLoopImpl::unRegisterTimer(sp_int64 timerId) {
  if (mTimerEvents.find(timerId) == mTimerEvents.end()) {
    // This fd wasn't registed at for reading. Hence we can't unregister it.
    return -1;
  }

  // Delete the underlying event in libevent
  if (event_del(mTimerEvents[timerId]->event()) != 0) {
    // cout << "event_del failed for timer " << timerId;
    throw heron::error::Error_Exception(errno);
  }
  delete mTimerEvents[timerId];
  mTimerEvents.erase(timerId);
  return 0;
}

void EventLoopImpl::registerInstantCallback(VCallback<> cb) {
  // Register the callback
  mListInstantCallbacks.push_back(std::move(cb));

  // Do we have a zero timer already going?
  if (mInstantZeroTimerId == -1) {
    mInstantZeroTimerId = getNextTimerId();
    auto instant_cb = [this](EventLoop::Status s) { this->handleInstantCallback(s); };

    // Create and init the appropriate data structures
    auto* event =
        new SS_RegisteredEvent<sp_int64>(mInstantZeroTimerId, false, std::move(instant_cb), 0);

    // TODO(vikasr): Convert this to VCallback
    CallBack1<sp_int16>* cbS =
        CreateCallback(this, &EventLoopImpl::handleTimerCallback, mInstantZeroTimerId);

    evtimer_set(event->event(), &eventLoopImplTimerCallback, cbS);
    if (event_base_set(mDispatcher, event->event()) < 0) {
      // cout << "event_base_set failed for timer " << mInstantZeroTimerId;
      delete event;
      delete cbS;
      throw heron::error::Error_Exception(errno);
    }

    // Now add the timer to libevent
    if (evtimer_add(event->event(), event->timer()) < 0) {
      LOG(ERROR) << "event_add failed for timer " << mInstantZeroTimerId;
      delete event;
      delete cbS;
      throw heron::error::Error_Exception(errno);
    }
    mTimerEvents[mInstantZeroTimerId] = event;
  }
}

void EventLoopImpl::handleInstantCallback(Status) {
  // Make sure that we don't invoke cb's that get added as part of invocation of
  // other callbacks.
  mInstantZeroTimerId = -1;
  auto enditr = --(mListInstantCallbacks.end());

  for (auto itr = mListInstantCallbacks.begin();; ++itr) {
    (*itr)();
    if (itr == enditr) break;
  }
  mListInstantCallbacks.erase(mListInstantCallbacks.begin(), ++enditr);
}

void EventLoopImpl::handleReadCallback(sp_int32 fd, sp_int16 event) {
  if (mReadEvents.find(fd) == mReadEvents.end()) {
    // This is possible when UnRegisterEvent has been called before we handle this event
    // Just ignore this event.
    LOG(ERROR)
        << "Got a Read Callback for an fd that is not registered. Probably unregistered? already";
    return;
  }

  SS_RegisteredEvent<sp_int32>* registeredEvent = mReadEvents[fd];

  if (registeredEvent->isPersistent()) {
    registeredEvent->get_callback()(mapStatusCode(event));
  } else {
    auto cb = std::move(registeredEvent->get_callback());
    // first clean up event if it is not persistent
    delete registeredEvent;
    mReadEvents.erase(fd);
    cb(mapStatusCode(event));
  }
}

void EventLoopImpl::handleWriteCallback(sp_int32 fd, sp_int16 event) {
  if (mWriteEvents.find(fd) == mWriteEvents.end()) {
    // This is possible when UnRegisterEvent has been called before we handle this event
    // Just ignore this event.
    // cout << "Got a Write Callback for an fd that is not registered. Probably unregistered
    // already?";
    return;
  }

  auto* registeredEvent = mWriteEvents[fd];

  if (registeredEvent->isPersistent()) {
    registeredEvent->get_callback()(mapStatusCode(event));
  } else {
    auto cb = std::move(registeredEvent->get_callback());
    // first clean up event if it is not persistent
    delete registeredEvent;
    mWriteEvents.erase(fd);
    cb(mapStatusCode(event));
  }
}

void EventLoopImpl::handleTimerCallback(sp_int16 event, sp_int64 timerId) {
  if (mTimerEvents.find(timerId) == mTimerEvents.end()) {
    // This is possible when unRegisterTimer has been called before we handle this timer
    // Just ignore this event.
    LOG(ERROR) << "Got a Write Callback for a timer that is not registered. Probably unregistered "
                  "already?";
    return;
  }

  auto* registeredEvent = mTimerEvents[timerId];

  if (registeredEvent->isPersistent()) {
    // we need to set the timer again
    CHECK_EQ(evtimer_add(registeredEvent->event(), registeredEvent->timer()), 0);
    registeredEvent->get_callback()(mapStatusCode(event));
  } else {
    auto cb = std::move(registeredEvent->get_callback());
    // first clean up event if it is not persistent
    delete registeredEvent;
    mTimerEvents.erase(timerId);
    cb(mapStatusCode(event));
  }
}

EventLoop::Status EventLoopImpl::mapStatusCode(sp_int16 event) {
  switch (event) {
    case EV_READ:
      return READ_EVENT;
    case EV_WRITE:
      return WRITE_EVENT;
    case EV_SIGNAL:
      return SIGNAL_EVENT;
    case EV_TIMEOUT:
      return TIMEOUT_EVENT;
    default:
      return UNKNOWN_EVENT;
  }
}
