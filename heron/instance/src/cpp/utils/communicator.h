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
// This file defines the Communicator class.
// Communicator is a mechanism used to communicate objects across
// different eventLoops/threads. Consumers setup an eventLoop and a
// consuming function while they setup the piper. Producers can enqueue
// items from any thread that will then magically be consumed
//
///////////////////////////////////////////////////////////////////////////////
#ifndef HERON_INSTANCE_SRC_CPP_UTILS_COMMUNICATOR_H_
#define HERON_INSTANCE_SRC_CPP_UTILS_COMMUNICATOR_H_

#include <fcntl.h>
#include <errno.h>

#include "basics/basics.h"
#include "threads/threads.h"
#include "network/event_loop.h"

namespace heron {
namespace instance {

/*
 * Communicator class definition
 */
template<typename T>
class Communicator {
 public:
  // Constructor/Destructor
  Communicator(std::shared_ptr<EventLoop> eventLoop, std::function<void(T)> consumer)
    : eventLoop_(eventLoop), consumer_(std::move(consumer)), registered_for_read_(false) {
    queue_ = new PCQueue<T>();
    if (pipe(pipers_) < 0) {
      LOG(FATAL) << "Pipe failed in Communicator";
    }
    sp_int32 flags;
    if ((flags = fcntl(pipers_[0], F_GETFL, 0)) < 0 ||
        fcntl(pipers_[0], F_SETFL, flags | O_NONBLOCK) < 0) {
      LOG(FATAL) << "fcntl failed in Communicator";
    }
    registerForRead();
  }

  virtual ~Communicator() {
    CHECK_EQ(eventLoop_->unRegisterForRead(pipers_[0]), 0);
    close(pipers_[0]);
    close(pipers_[1]);
    delete queue_;
  }

  // The main interface of Piper. This will enqueue the item so that
  // the consumer function will pluck it and execute in the eventLoop
  void enqueue(T t) {
    queue_->enqueue(std::move(t));
    SignalMainThread();
  }

  int size() {
    return queue_->size();
  }

  bool registeredForRead() const { return registered_for_read_; }
  void unRegisterForRead() {
    if (!registered_for_read_) return;
    if (eventLoop_->unRegisterForRead(pipers_[0]) != 0) {
      LOG(FATAL) << "Could not unregister for read in Communicator";
    }
    registered_for_read_ = false;
  }

  void registerForRead() {
    if (registered_for_read_) return;
    auto wakeup_cb = [this](EventLoop::Status status) {
      this->OnWakeUp(status);
    };
    if (eventLoop_->registerForRead(pipers_[0], std::move(wakeup_cb), true) != 0) {
      LOG(FATAL) << "Could not register for read in Communicator";
    }
    registered_for_read_ = true;
  }

 private:
  // This is the function used to signal the main thread
  void SignalMainThread() {
    // This need not be protected by any mutex.
    // The os will take care of that.
    int rc = write(pipers_[1], "a", 1);
    if (rc != 1) {
      LOG(FATAL) << "Write to pipe failed in Communicator with return code:  " << rc;
    }
  }


  // This is the function that is registered in the event loop
  // to be called when awoken
  void OnWakeUp(EventLoop::Status status) {
    if (status == EventLoop::READ_EVENT) {
      char buf[1];
      ssize_t readcount = read(pipers_[0], buf, 1);
      if (readcount == 1) {
        bool dequeued = false;
        T t = queue_->trydequeue(dequeued);
        if (dequeued) {
          consumer_(std::move(t));
        }
      } else {
        LOG(ERROR) << "In Communicator read from pipers returned "
                   << readcount << " errno " << errno;
        if (readcount < 0 && (errno == EAGAIN || errno == EINTR)) {
          // Never mind. we will try again
          return;
        } else {
          // We really don't know what to do here.
          return;
        }
      }
    }
    return;
  }

  std::shared_ptr<EventLoop> eventLoop_;

  // These pipers are how they communicate it accross to our thread
  sp_int32 pipers_[2];
  // This is where callbacks are queued
  PCQueue<T>* queue_;
  // This is the consumer function
  std::function<void(T)> consumer_;
  bool registered_for_read_;
};

}  // end namespace instance
}  // end namespace heron

#endif  // HERON_INSTANCE_SRC_CPP_UTILS_COMMUNICATOR_H_
