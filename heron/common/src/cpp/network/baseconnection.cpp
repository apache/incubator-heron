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

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////

#include "network/baseconnection.h"
#include <string>
#include "glog/logging.h"
#include "basics/basics.h"

BaseConnection::BaseConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                               EventLoop* eventLoop)
    : mOptions(options), mEndpoint(endpoint), mEventLoop(eventLoop) {
  mState = INIT;
  mReadState = NOTREGISTERED;
  mWriteState = NOTREGISTERED;
  mOnClose = NULL;
  mOnRead = [this](EventLoop::Status s) { return this->handleRead(s); };
  mOnWrite = [this](EventLoop::Status s) { return this->handleWrite(s); };
  mCanCloseConnection = true;
}

BaseConnection::~BaseConnection() { CHECK(mState == INIT || mState == DISCONNECTED); }

sp_int32 BaseConnection::start() {
  if (mState != INIT) {
    LOG(ERROR) << "Connection not in INIT State, hence cannot start\n";
    return -1;
  }
  if (registerEndpointForRead() < 0) {
    LOG(ERROR) << "Could not register for read of the socket during start\n";
    return -1;
  }
  mState = CONNECTED;
  return 0;
}

void BaseConnection::closeConnection() {
  if (mState != CONNECTED) {
    // Nothing to do here
    LOG(ERROR) << "Connection already closed, hence doing nothing\n";
    return;
  }
  mState = TO_BE_DISCONNECTED;
  internalClose();
}

void BaseConnection::internalClose() {
  if (mState != TO_BE_DISCONNECTED) return;
  if (!mCanCloseConnection) return;
  mState = DISCONNECTED;

  // First set the status that we are going to send any outstanding Send callbacks
  NetworkErrorCode status;
  if (mReadState == ERROR) {
    status = READ_ERROR;
  } else if (mWriteState == ERROR) {
    status = WRITE_ERROR;
  } else {
    status = OK;
  }

  // If state is alredy scheduled to be closed, we should not attempt
  // to unsubscribe read.
  if (mReadState != NOTREGISTERED) {
    CHECK_EQ(unregisterEndpointForRead(), 0);
  }
  if (mWriteState == NOTREADY) {
    sp_int32 writeUnsubscribe = mEventLoop->unRegisterForWrite(mEndpoint->get_fd());
    CHECK_EQ(writeUnsubscribe, 0);
  }
  mWriteState = NOTREGISTERED;

  // close the socket
  sp_int32 retval = close(mEndpoint->get_fd());

  if (retval != 0) {
    // Ok close failed. What do we do
    LOG(ERROR) << "Close of the connection socket failed\n";
    if (mOnClose) {
      mOnClose(CLOSE_ERROR);
    }
  } else {
    if (mOnClose) {
      mOnClose(status);
    }
  }
}

sp_int32 BaseConnection::registerForWrite() {
  if (mState != CONNECTED) {
    LOG(ERROR) << "Connection is not connected, hence cannot send\n";
    return -1;
  }
  if (mWriteState == NOTREGISTERED) {
    CHECK_EQ(mEventLoop->registerForWrite(mEndpoint->get_fd(), mOnWrite, false), 0);
    mWriteState = NOTREADY;
  }
  return 0;
}

void BaseConnection::registerForClose(VCallback<NetworkErrorCode> cb) { mOnClose = std::move(cb); }

// Note that we hold the mutex when we come to this function
void BaseConnection::handleWrite(EventLoop::Status status) {
  CHECK_EQ(status, EventLoop::WRITE_EVENT);
  mWriteState = NOTREGISTERED;

  if (mState != CONNECTED) return;

  sp_int32 writeStatus = writeIntoEndPoint(mEndpoint->get_fd());
  if (writeStatus < 0) {
    mWriteState = ERROR;
    mState = TO_BE_DISCONNECTED;
  }
  if (mState == CONNECTED && mWriteState == NOTREGISTERED && stillHaveDataToWrite()) {
    mWriteState = NOTREADY;
    CHECK_EQ(mEventLoop->registerForWrite(mEndpoint->get_fd(), mOnWrite, false), 0);
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  handleDataWritten();
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
}

void BaseConnection::handleRead(EventLoop::Status status) {
  CHECK(status == EventLoop::READ_EVENT);
  mReadState = READY;
  sp_int32 readStatus = readFromEndPoint(mEndpoint->get_fd());
  if (readStatus >= 0) {
    mReadState = NOTREADY;
  } else {
    mReadState = ERROR;
    mState = TO_BE_DISCONNECTED;
  }

  bool prevValue = mCanCloseConnection;
  mCanCloseConnection = false;
  handleDataRead();
  mCanCloseConnection = prevValue;
  if (mState != CONNECTED) {
    internalClose();
  }
}

sp_string BaseConnection::getIPAddress() {
  std::string addr_result;
  if (!mEndpoint->is_unix_socket()) {
    char addr_str[INET_ADDRSTRLEN];
    struct sockaddr_in* addr_in = (struct sockaddr_in*)(mEndpoint->addr());
    if (inet_ntop(addr_in->sin_family, &(addr_in->sin_addr), addr_str, INET_ADDRSTRLEN)) {
      addr_result = addr_str;
    } else {
      addr_result = "";
    }
  } else {
    struct sockaddr_un* addr_un = (struct sockaddr_un*)(mEndpoint->addr());
    addr_result = addr_un->sun_path;
  }
  return addr_result;
}

sp_int32 BaseConnection::getPort() {
  if (!mEndpoint->is_unix_socket()) {
    return ntohs(((struct sockaddr_in*)(mEndpoint->addr()))->sin_port);
  } else {
    return -1;
  }
}

sp_int32 BaseConnection::unregisterEndpointForRead() {
  if (mEventLoop->unRegisterForRead(mEndpoint->get_fd()) < 0) {
    LOG(ERROR) << "Could not remove fd from read";
    return -1;
  }
  mReadState = NOTREGISTERED;
  return 0;
}

sp_int32 BaseConnection::registerEndpointForRead() {
  if (mEventLoop->registerForRead(mEndpoint->get_fd(), mOnRead, true) < 0) {
    return -1;
  }
  mReadState = NOTREADY;
  return 0;
}
