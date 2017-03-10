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

#include "network/connection.h"
#include <algorithm>
#include <list>
#include <utility>

#if defined(__APPLE__)
#include <sys/uio.h>
#endif

#include "glog/logging.h"

const sp_int32 __SYSTEM_NETWORK_READ_BATCH_SIZE__ = 1048576;           // 1M
const sp_int32 __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__ = 1048576;  // 1M

// How many times should we wait to see a buffer full while enqueueing data
// before declaring start of back pressure
const sp_uint8 __SYSTEM_MIN_NUM_ENQUEUES_WITH_BUFFER_FULL__ = 3;

Connection::Connection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                       EventLoop* eventLoop)
    : BaseConnection(endpoint, options, eventLoop) {
  mIncomingPacket = new IncomingPacket(mOptions->max_packet_size_);
  mOnNewPacket = NULL;
  mOnConnectionBufferEmpty = NULL;
  mOnConnectionBufferFull = NULL;
  mNumOutstandingBytes = 0;
  mIOVectorSize = 1024;
  mIOVector = new struct iovec[mIOVectorSize];

  mWriteBatchsize = __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__;
  mCausedBackPressure = false;
  mUnderBackPressure = false;
  mNumEnqueuesWithBufferFull = 0;
}

Connection::~Connection() {
  if (hasCausedBackPressure()) {
    mOnConnectionBufferEmpty(this);
  }
  delete mIncomingPacket;
  {
    while (!mOutstandingPackets.empty()) {
      auto p = mOutstandingPackets.front();
      delete p;
      mOutstandingPackets.pop_front();
    }
    while (!mSentPackets.empty()) {
      auto p = mSentPackets.front();
      delete p;
      mSentPackets.pop();
    }
  }
  while (!mReceivedPackets.empty()) {
    auto p = mReceivedPackets.front();
    delete p;
    mReceivedPackets.pop();
  }
  delete[] mIOVector;
}

sp_int32 Connection::sendPacket(OutgoingPacket* packet) {
  packet->PrepareForWriting();
  if (registerForWrite() != 0) return -1;
  mOutstandingPackets.push_back(packet);
  mNumOutstandingBytes += packet->GetTotalPacketSize();

  if (!hasCausedBackPressure()) {
    // Are we above the threshold?
    if (mNumOutstandingBytes >= mOptions->high_watermark_) {
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

sp_int32 Connection::writeIntoIOVector(sp_int32 maxWrite, sp_int32* toWrite) {
  sp_uint32 bytesLeft = maxWrite;
  sp_int32 simulWrites = std::min(mIOVectorSize, (sp_int32)mOutstandingPackets.size());
  *toWrite = 0;
  auto iter = mOutstandingPackets.begin();
  for (sp_int32 i = 0; i < simulWrites; ++i) {
    mIOVector[i].iov_base = (*iter)->get_header() + (*iter)->position_;
    mIOVector[i].iov_len = PacketHeader::get_packet_size((*iter)->get_header()) +
                           PacketHeader::header_size() - (*iter)->position_;
    if (mIOVector[i].iov_len >= bytesLeft) {
      mIOVector[i].iov_len = bytesLeft;
    }
    bytesLeft -= mIOVector[i].iov_len;
    *toWrite = *toWrite + mIOVector[i].iov_len;
    if (bytesLeft <= 0) {
      return i + 1;
    }
    iter++;
  }
  return simulWrites;
}

void Connection::afterWriteIntoIOVector(sp_int32 simulWrites, ssize_t numWritten) {
  mNumOutstandingBytes -= numWritten;

  for (sp_int32 i = 0; i < simulWrites; ++i) {
    auto pr = mOutstandingPackets.front();
    if (numWritten >= (ssize_t)mIOVector[i].iov_len) {
      // This iov structure was completely written as instructed
      sp_uint32 bytesLeftForThisPacket = PacketHeader::get_packet_size(pr->get_header()) +
                                         PacketHeader::header_size() - pr->position_;
      bytesLeftForThisPacket -= mIOVector[i].iov_len;
      if (bytesLeftForThisPacket == 0) {
        // This whole packet has been consumed
        mSentPackets.push(pr);
        mOutstandingPackets.pop_front();
      } else {
        pr->position_ += mIOVector[i].iov_len;
      }
      numWritten -= mIOVector[i].iov_len;
    } else {
      // This iov structure has been partially sent out
      pr->position_ += numWritten;
      numWritten = 0;
    }
    if (numWritten <= 0) break;
  }

  // Check if we reduced the write buffer to something below the back
  // pressure threshold
  if (hasCausedBackPressure()) {
    // Signal pipe free
    if (mNumOutstandingBytes <= mOptions->low_watermark_) {
      mOnConnectionBufferEmpty(this);
    }
  }
}

bool Connection::stillHaveDataToWrite() {
  if (mOutstandingPackets.empty()) return false;
  return true;
}

sp_int32 Connection::writeIntoEndPoint(sp_int32 fd) {
  sp_int32 bytesWritten = 0;
  while (1) {
    sp_int32 stillToWrite = mWriteBatchsize - bytesWritten;
    sp_int32 toWrite = 0;
    sp_int32 simulWrites = writeIntoIOVector(stillToWrite, &toWrite);

    ssize_t numWritten = ::writev(fd, mIOVector, simulWrites);
    if (numWritten >= 0) {
      afterWriteIntoIOVector(simulWrites, numWritten);
      bytesWritten += numWritten;
      if (bytesWritten >= mWriteBatchsize) {
        // We only write a at max this bytes at a time.
        // This is so that others can get a chance
        return 0;
      }
      if (numWritten < toWrite) {
        // writev would block otherwise
        return 0;
      }
      if (!stillHaveDataToWrite()) {
        // No more packets to write
        return 0;
      }
    } else {
      // some error happened in writev
      if (errno == EAGAIN || errno == EINTR) {
        // we need to retry the write again
        LOG(INFO) << "writev said to try again\n";
      } else {
        LOG(ERROR) << "error happened in writev " << errno << "\n";
        return -1;
      }
    }
  }
}

void Connection::handleDataWritten() {
  while (!mSentPackets.empty()) {
    auto pr = mSentPackets.front();
    delete pr;
    mSentPackets.pop();
  }
}

sp_int32 Connection::readFromEndPoint(sp_int32 fd) {
  sp_int32 bytesRead = 0;
  while (1) {
    sp_int32 read_status = mIncomingPacket->Read(fd);
    if (read_status == 0) {
      // Packet was succcessfully read.
      IncomingPacket* packet = mIncomingPacket;
      mIncomingPacket = new IncomingPacket(mOptions->max_packet_size_);
      mReceivedPackets.push(packet);
      bytesRead += packet->GetTotalPacketSize();
      if (bytesRead >= __SYSTEM_NETWORK_READ_BATCH_SIZE__) {
        return 0;
      }
    } else if (read_status > 0) {
      // packet was read partially
      return 0;
    } else {
      return -1;
    }
  }
}

void Connection::handleDataRead() {
  while (!mReceivedPackets.empty()) {
    IncomingPacket* packet = mReceivedPackets.front();
    if (mOnNewPacket) {
      mOnNewPacket(packet);
    } else {
      delete packet;
    }
    mReceivedPackets.pop();
  }
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
