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
// Implements the Piper class. See piper.h for API details.
///////////////////////////////////////////////////////////////////////////////
#include "network/piper.h"
#include <fcntl.h>
#include <errno.h>
#include "glog/logging.h"

void RunUserCb(VCallback<> cb) { cb(); }

// Constructor. We create a new event_base
Piper::Piper(std::shared_ptr<EventLoop> eventLoop)
    : eventLoop_(eventLoop) {
  cbs_ = new PCQueue<CallBack*>();
  auto wakeup_cb = [this](EventLoop::Status status) {
    this->OnWakeUp(status);
  };

  if (pipe(pipers_) < 0) {
    LOG(FATAL) << "Pipe failed in Piper";
  }
  sp_int32 flags;
  if ((flags = fcntl(pipers_[0], F_GETFL, 0)) < 0 ||
      fcntl(pipers_[0], F_SETFL, flags | O_NONBLOCK) < 0 ||
      eventLoop_->registerForRead(pipers_[0], std::move(wakeup_cb), true) != 0) {
    LOG(FATAL) << "fcntl failed in piper";
  }
}

// Destructor.
Piper::~Piper() {
  CHECK_EQ(eventLoop_->unRegisterForRead(pipers_[0]), 0);
  close(pipers_[0]);
  close(pipers_[1]);
  delete cbs_;
}

//
// Interface implementation
//

void Piper::ExecuteInEventLoop(VCallback<> _cb) {
  cbs_->enqueue(CreateCallback(&RunUserCb, std::move(_cb)));
  SignalMainThread();
}

//
// Internal functions
//
void Piper::SignalMainThread() {
  // This need not be protected by any mutex.
  // Ths os will take care of that.
  int rc = write(pipers_[1], "a", 1);
  if (rc != 1) {
    LOG(FATAL) << "Write to pipe failed in Piper with return code: " << rc;
  }
}

void Piper::OnWakeUp(EventLoop::Status _status) {
  if (_status == EventLoop::READ_EVENT) {
    char buf[1];
    ssize_t readcount = read(pipers_[0], buf, 1);
    if (readcount == 1) {
      bool dequeued = false;
      CallBack* cb = reinterpret_cast<CallBack*>(cbs_->trydequeue(dequeued));
      if (cb) {
        cb->Run();
      }
    } else {
      LOG(ERROR) << "In Server read from pipers returned " << readcount << " errno " << errno
                << std::endl;
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
