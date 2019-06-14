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

////////////////////////////////////////////////////////////////////////////////
//
////////////////////////////////////////////////////////////////////////////////

#include "network/baseconnection.h"
#include <string>
#include "basics/basics.h"
#include "glog/logging.h"
#include "network/regevent.h"

const sp_int32 __SYSTEM_NETWORK_READ_BATCH_SIZE__ = 1048576;           // 1M
const sp_int32 __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__ = 1048576;  // 1M

// 'C' style callback for libevent on read events
void readcb(struct bufferevent *bev, void *ctx) {
  auto* conn = reinterpret_cast<BaseConnection*>(ctx);
  conn->handleRead();
}

// 'C' style callback for libevent on write events
void writecb(struct bufferevent *bev, void *ctx) {
  auto* conn = reinterpret_cast<BaseConnection*>(ctx);
  conn->handleWrite();
}

void eventcb(struct bufferevent *bev, sp_int16 events, void *ctx) {
  auto* conn = reinterpret_cast<BaseConnection*>(ctx);
  conn->handleEvent(events);
}

BaseConnection::BaseConnection(ConnectionEndPoint* endpoint, ConnectionOptions* options,
                               std::shared_ptr<EventLoop> eventLoop)
    : mOptions(options), mEndpoint(endpoint), mEventLoop(eventLoop) {
  mState = INIT;
  mOnClose = NULL;
  bufferevent_options boptions = BEV_OPT_DEFER_CALLBACKS;
  buffer_ = bufferevent_socket_new(mEventLoop->dispatcher(), mEndpoint->get_fd(), boptions);
  read_bps_ = burst_read_bps_ = 0;
  rate_limit_cfg_ = NULL;
}

BaseConnection::~BaseConnection() {
  CHECK(mState == INIT || mState == DISCONNECTED);
  disableRateLimit();  // To free the config object
  bufferevent_free(buffer_);
}

sp_int32 BaseConnection::start() {
  if (mState != INIT) {
    LOG(ERROR) << "Connection not in INIT State, hence cannot start\n";
    return -1;
  }
  bufferevent_setwatermark(buffer_, EV_WRITE, mOptions->low_watermark_, 0);
  CHECK_EQ(bufferevent_set_max_single_read(buffer_, __SYSTEM_NETWORK_READ_BATCH_SIZE__), 0);
  CHECK_EQ(bufferevent_set_max_single_write(buffer_,
                                            __SYSTEM_NETWORK_DEFAULT_WRITE_BATCH_SIZE__), 0);
  bufferevent_setcb(buffer_, readcb, writecb, eventcb, this);
  if (bufferevent_enable(buffer_, EV_READ|EV_WRITE) < 0) {
    LOG(ERROR) << "Could not register for read/write of the buffer during start\n";
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
  internalClose(OK);
}

void BaseConnection::internalClose(NetworkErrorCode status) {
  if (mState != TO_BE_DISCONNECTED) return;
  mState = DISCONNECTED;

  bufferevent_disable(buffer_, EV_READ|EV_WRITE);

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

void BaseConnection::registerForClose(VCallback<NetworkErrorCode> cb) { mOnClose = std::move(cb); }

void BaseConnection::handleWrite() {
  releiveBackPressure();
}

void BaseConnection::handleRead() {
  sp_int32 readStatus = readFromEndPoint(buffer_);
  if (readStatus < 0) {
    mState = TO_BE_DISCONNECTED;
    internalClose(READ_ERROR);
  }
}

sp_int32 BaseConnection::write(struct evbuffer* _buffer) {
  int retval = bufferevent_write_buffer(buffer_, _buffer);
  evbuffer_free(_buffer);
  return retval;
}

void BaseConnection::handleEvent(sp_int16 events) {
  if (events & BEV_EVENT_CONNECTED) {
    LOG(FATAL) << "BaseConnetion does not process connected event";
  }
  if (events & (BEV_EVENT_ERROR|BEV_EVENT_EOF)) {
    LOG(ERROR) << "BufferEvent reported error on connection " << this;
    mState = TO_BE_DISCONNECTED;
    internalClose(WRITE_ERROR);
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
  LOG(INFO) << "Unregistering for read for " << this;
  return bufferevent_disable(buffer_, EV_READ);
}

sp_int32 BaseConnection::registerEndpointForRead() {
  LOG(INFO) << "Re registereing for read for " << this;
  return bufferevent_enable(buffer_, EV_READ);
}

sp_int32 BaseConnection::getOutstandingBytes() const {
  return evbuffer_get_length(bufferevent_get_output(buffer_));
}

bool BaseConnection::setRateLimit(const sp_int64 _read_bps, const sp_int64 _burst_read_bps) {
  if (_read_bps > 0 && _burst_read_bps > 0) {
    if (_read_bps != read_bps_ || _burst_read_bps != burst_read_bps_) {
      // Create new config
      struct ev_token_bucket_cfg* new_rate_limit_cfg = ev_token_bucket_cfg_new(
          _read_bps, _burst_read_bps, EV_RATE_LIMIT_MAX, EV_RATE_LIMIT_MAX, NULL);
      if (bufferevent_set_rate_limit(buffer_, new_rate_limit_cfg) == -1) {
        ev_token_bucket_cfg_free(new_rate_limit_cfg);
        LOG(ERROR) << "Faild to apply rate limiting to bufferevent ";
        return false;
      }
      // Update internal data
      ev_token_bucket_cfg_free(rate_limit_cfg_);
      rate_limit_cfg_ = new_rate_limit_cfg;
      read_bps_ = _read_bps;
      burst_read_bps_ = _burst_read_bps;
    }
    return true;
  }
  return false;
}

void BaseConnection::disableRateLimit() {
  bufferevent_set_rate_limit(buffer_, NULL);
  if (rate_limit_cfg_) {
    read_bps_ = burst_read_bps_ = 0;
    ev_token_bucket_cfg_free(rate_limit_cfg_);
    rate_limit_cfg_ = NULL;
  }
}
