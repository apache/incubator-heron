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
// Implements the BaseClient class. See baseclient.h for details on the API
////////////////////////////////////////////////////////////////////////////////

#include <netdb.h>
#include "network/baseclient.h"
#include "glog/logging.h"
#include "basics/basics.h"

BaseClient::BaseClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options) {
  Init(eventLoop, _options);
}

void BaseClient::Init(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options) {
  eventLoop_ = eventLoop;
  options_ = _options;
  conn_ = NULL;
  connection_options_.max_packet_size_ = options_.get_max_packet_size();
  connection_options_.high_watermark_ = options_.get_high_watermark();
  connection_options_.low_watermark_ = options_.get_low_watermark();
  state_ = DISCONNECTED;
}

BaseClient::~BaseClient() {}

void BaseClient::Start_Base() {
  if (state_ != DISCONNECTED) {
    LOG(ERROR) << "Attempting to start a client which is already in state_ " << state_ << "\n";
    HandleConnect_Base(DUPLICATE_START);
    return;
  }
  // open a socket
  int fd = -1;
  fd = socket(options_.get_socket_family(), SOCK_STREAM, 0);
  if (fd < 0) {
    PLOG(ERROR) << "Opening of socket failed in Client";
    HandleConnect_Base(CONNECT_ERROR);
    return;
  }

  // set default socket options
  if (SockUtils::setSocketDefaults(fd) < 0) {
    close(fd);
    HandleConnect_Base(CONNECT_ERROR);
    return;
  }

  // construct an endpoint to store the connection information
  auto endpoint = new ConnectionEndPoint(options_.get_socket_family() != PF_INET);
  endpoint->set_fd(fd);
  bzero(reinterpret_cast<char*>(endpoint->addr()), endpoint->addrlen());
  // Set the address
  if (options_.get_socket_family() == PF_INET) {
    struct sockaddr_in* addr = (struct sockaddr_in*)endpoint->addr();
    addr->sin_family = AF_INET;
    addr->sin_port = htons(options_.get_port());
    struct sockaddr_in t;
    int error = IpUtils::getAddressInfo(t, options_.get_host().c_str(), PF_INET, SOCK_STREAM);
    if (error) {
      LOG(ERROR) << "getaddrinfo failed in Client " << options_.get_host()
          << ": "<< gai_strerror(error);
      close(fd);
      delete endpoint;
      HandleConnect_Base(CONNECT_ERROR);
      return;
    }
    memcpy(&(addr->sin_addr), &(t.sin_addr), sizeof(addr->sin_addr));
  } else {
    struct sockaddr_un* addr = (struct sockaddr_un*)endpoint->addr();
    addr->sun_family = options_.get_sin_family();
    snprintf(addr->sun_path, sizeof(addr->sun_path), "%s", options_.get_sin_path().c_str());
  }

  // connect to the address
  errno = 0;
  if (connect(endpoint->get_fd(), endpoint->addr(), endpoint->addrlen()) == 0 ||
      errno == EINPROGRESS) {
    state_ = CONNECTING;
    // Either connect succeeded or it woud block
    auto cb = [endpoint, this](EventLoop::Status status) { this->OnConnect(endpoint, status); };

    CHECK_EQ(eventLoop_->registerForWrite(endpoint->get_fd(), std::move(cb), false), 0);
    return;
  } else {
    // connect failed. Bail out saying that the start failed.
    PLOG(ERROR) << "Connect failed";
    close(endpoint->get_fd());
    delete endpoint;
    HandleConnect_Base(CONNECT_ERROR);
    return;
  }
}

void BaseClient::OnConnect(ConnectionEndPoint* _endpoint, EventLoop::Status _status) {
  sp_int32 error = 0;
  socklen_t len = sizeof(error);

  // If either we got an event other that write event or the connect didnt succeed
  // we need to close shop.
  if (_status != EventLoop::WRITE_EVENT ||
      getsockopt(_endpoint->get_fd(), SOL_SOCKET, SO_ERROR, reinterpret_cast<void*>(&error), &len) <
          0 ||
      error != 0) {
    // we asked for a write event but select server delivered something else.
    // or for some reason the connect failed
    close(_endpoint->get_fd());
    delete _endpoint;
    state_ = DISCONNECTED;
    HandleConnect_Base(CONNECT_ERROR);
    return;
  }

  // Init the connection and start it
  conn_ = CreateConnection(_endpoint, &connection_options_, eventLoop_);
  if (conn_->start() != 0) {
    close(_endpoint->get_fd());
    delete conn_;
    conn_ = NULL;
    state_ = DISCONNECTED;
    HandleConnect_Base(CONNECT_ERROR);
    return;
  }

  state_ = CONNECTED;
  conn_->registerForClose([this](NetworkErrorCode s) { this->OnClose(s); });
  HandleConnect_Base(OK);
  return;
}

void BaseClient::Stop_Base() {
  if (state_ == DISCONNECTED || state_ == CONNECTING) {
    // This is a noop
    return;
  }
  state_ = DISCONNECTED;
  conn_->closeConnection();
}

void BaseClient::OnClose(NetworkErrorCode _status) {
  delete conn_;
  conn_ = NULL;
  state_ = DISCONNECTED;
  HandleClose_Base(_status);
}

sp_int64 BaseClient::AddTimer_Base(VCallback<> cb, sp_int64 _msecs) {
  auto eventCb = [cb, this](EventLoop::Status status) { this->OnTimer(std::move(cb), status); };

  sp_int64 timer_id = eventLoop_->registerTimer(std::move(eventCb), false, _msecs);
  CHECK_GT(timer_id, 0);
  return timer_id;
}

sp_int32 BaseClient::RemoveTimer_Base(sp_int64 timer_id) {
  return eventLoop_->unRegisterTimer(timer_id);
}

void BaseClient::OnTimer(VCallback<> cb, EventLoop::Status) { cb(); }
