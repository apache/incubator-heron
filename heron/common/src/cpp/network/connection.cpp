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

#include "network/connection.h"
#include <algorithm>
#include <list>
#include <utility>

#if defined(__APPLE__)
#include <sys/uio.h>
#endif

#include "glog/logging.h"
#include "network/regevent.h"

// How many times should we wait to see a buffer full while enqueueing data
// before declaring start of back pressure
const sp_uint8 __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__ = 3;

Connection::Connection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                       std::shared_ptr<EventLoop> eventLoop)
    : BaseConnection(endpoint, options, eventLoop) {
  mIncomingPacket = new IncomingPacket(mOptions->max_packet_size_);
  mOnNewPacket = NULL;
  mOnConnectionBufferEmpty = NULL;
  mOnConnectionBufferFull = NULL;
  mCausedBackPressure = false;
  mUnderBackPressure = false;
  mNumEnqueuesWithBufferFull = 0;
}

Connection::~Connection() {
  if (hasCausedBackPressure()) {
    mOnConnectionBufferEmpty(this);
  }
  delete mIncomingPacket;
}

sp_int32 Connection::sendPacket(OutgoingPacket* packet) {
  struct evbuffer* packet_buffer =  packet->release_buffer();
  delete packet;
  if (write(packet_buffer) < 0) return -1;

  if (!hasCausedBackPressure()) {
    // Are we above the threshold?
    if (getOutstandingBytes() >= mOptions->high_watermark_) {
      // Have we been above the threshold enough number of times?
      if (++mNumEnqueuesWithBufferFull > __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__) {
        mNumEnqueuesWithBufferFull = 0;
        if (mOnConnectionBufferFull) {
          mOnConnectionBufferFull(this);
        }
      }
    } else {
      mNumEnqueuesWithBufferFull = 0;
    }
  }
  return 0;
}

void Connection::registerForNewPacket(VCallback<IncomingPacket*> cb) {
  mOnNewPacket = std::move(cb);
}

sp_int32 Connection::registerForBackPressure(VCallback<Connection*> cbStarter,
                                             VCallback<Connection*> cbReliever) {
  mOnConnectionBufferFull = std::move(cbStarter);
  mOnConnectionBufferEmpty = std::move(cbReliever);
  return 0;
}

void Connection::releiveBackPressure() {
  // Check if we reduced the write buffer to something below the back
  // pressure threshold
  if (hasCausedBackPressure()) {
    mOnConnectionBufferEmpty(this);
  }
}

sp_int32 Connection::readFromEndPoint(struct bufferevent* _buffer) {
  std::queue<IncomingPacket*> received_packets;
  while (1) {
    sp_int32 read_status = mIncomingPacket->Read(_buffer);
    if (read_status == 0) {
      // Packet was succcessfully read.
      IncomingPacket* packet = mIncomingPacket;
      mIncomingPacket = new IncomingPacket(mOptions->max_packet_size_);
      received_packets.push(packet);
    } else if (read_status > 0) {
      // packet was read partially
      break;
    } else {
      return -1;
    }
  }
  while (!received_packets.empty()) {
    IncomingPacket* packet = received_packets.front();
    if (mOnNewPacket) {
      mOnNewPacket(packet);
    } else {
      delete packet;
    }
    received_packets.pop();
  }
  return 0;
}

sp_int32 Connection::putBackPressure() {
  mUnderBackPressure = true;
  // For now stop reads from this connection
  if (unregisterEndpointForRead() < 0) {
    LOG(ERROR) << "Could not start back pressure on connection";
    return -1;
  }
  return 0;
}

sp_int32 Connection::removeBackPressure() {
  mUnderBackPressure = false;
  // Resume reading from this connection
  if (registerEndpointForRead() < 0) {
    LOG(ERROR) << "Could not remove back pressure from connection";
    return -1;
  }
  return 0;
}
