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
// Implements the BaseServer class. See baseserver.h for details on the API
////////////////////////////////////////////////////////////////////////////////

#include "network/baseserver.h"
#include "glog/logging.h"
#include "basics/basics.h"

void CallHandleConnectionCloseAndDelete(BaseServer* _server, BaseConnection* _connection,
                                        NetworkErrorCode _status) {
  _server->HandleConnectionClose_Base(_connection, _status);
  delete _connection;
}

BaseServer::BaseServer(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options) {
  Init(eventLoop, _options);
}

void BaseServer::Init(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options) {
  eventLoop_ = eventLoop;
  options_ = _options;
  listen_fd_ = -1;
  connection_options_.max_packet_size_ = options_.get_max_packet_size();
  connection_options_.high_watermark_ = options_.get_high_watermark();
  connection_options_.low_watermark_ = options_.get_low_watermark();
  on_new_connection_callback_ = [this](EventLoop::Status status) { this->OnNewConnection(status); };
}

BaseServer::~BaseServer() {}

sp_int32 BaseServer::Start_Base() {
  // open a socket
  errno = 0;
  listen_fd_ = socket(options_.get_socket_family(), SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    LOG(ERROR) << "Opening of a socket failed in server " << errno << "\n";
    return -1;
  }

  if (SockUtils::setSocketDefaults(listen_fd_) < 0) {
    close(listen_fd_);
    return -1;
  }

  // Set the socket option for addr reuse
  if (SockUtils::setReuseAddress(listen_fd_) < 0) {
    LOG(ERROR) << "setsockopt of a socket failed in server " << errno << "\n";
    close(listen_fd_);
    return -1;
  }

  // Set the address
  struct sockaddr_in in_addr;
  struct sockaddr_un unix_addr;
  struct sockaddr* serv_addr = NULL;
  socklen_t sockaddr_len = 0;
  if (options_.get_sin_family() == AF_INET) {
    bzero(reinterpret_cast<char*>(&in_addr), sizeof(in_addr));
    in_addr.sin_family = options_.get_sin_family();
    in_addr.sin_port = htons(options_.get_port());
    in_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr = (struct sockaddr*)&in_addr;
    sockaddr_len = sizeof(in_addr);
  } else {
    bzero(reinterpret_cast<char*>(&unix_addr), sizeof(unix_addr));
    unix_addr.sun_family = options_.get_sin_family();
    snprintf(unix_addr.sun_path, sizeof(unix_addr.sun_path), "%s", options_.get_sin_path().c_str());
    serv_addr = (struct sockaddr*)&unix_addr;
    sockaddr_len = sizeof(unix_addr);
  }

  // Bind to the address
  if (bind(listen_fd_, serv_addr, sockaddr_len) < 0) {
    LOG(ERROR) << "bind of a socket failed in server " << errno << "\n";
    close(listen_fd_);
    return -1;
  }

  // Listen for new connections
  if (listen(listen_fd_, 100) < 0) {
    LOG(ERROR) << "listen of a socket failed in server " << errno << "\n";
    close(listen_fd_);
    return -1;
  }

  // Ask the EventLoop to deliver any read events
  if (eventLoop_->registerForRead(listen_fd_, on_new_connection_callback_, true) < 0) {
    LOG(ERROR) << "register for read of the socket failed in server\n";
    close(listen_fd_);
    return -1;
  }

  // fetch the port after bind/listen port 0
  if (AF_INET == options_.get_sin_family() && 0 == options_.get_port()) {
    if (getsockname(listen_fd_, serv_addr, &sockaddr_len) != 0)  {
      LOG(ERROR) << "getsockname() error: " << strerror(errno);
      close(listen_fd_);
      return SP_NOTOK;
    } else {
      options_.set_port(ntohs(in_addr.sin_port));
    }
  }

  return 0;
}

sp_int32 BaseServer::Stop_Base() {
  // Stop accepting new connections
  CHECK_EQ(eventLoop_->unRegisterForRead(listen_fd_), 0);
  // Close the listen socket.
  close(listen_fd_);

  // Close all active connections and delete them
  while (active_connections_.size() > 0) {
    BaseConnection* conn = *(active_connections_.begin());
    conn->closeConnection();
    // Note:- we don't delete the connection here. They are deleted in
    // the OnConnectionClose call.
  }
  CHECK(active_connections_.empty());

  return 0;
}

void BaseServer::OnNewConnection(EventLoop::Status _status) {
  if (_status == EventLoop::READ_EVENT) {
    // The EventLoop indicated that the socket is writable.
    // Which means that a new client has connected to it.
    auto endPoint = new ConnectionEndPoint(options_.get_sin_family() != AF_INET);
    struct sockaddr* serv_addr = endPoint->addr();
    socklen_t addrlen = endPoint->addrlen();
    sp_int32 fd = accept(listen_fd_, serv_addr, &addrlen);
    endPoint->set_fd(fd);
    if (endPoint->get_fd() > 0) {
      // accept succeeded.

      // Set defaults
      if (SockUtils::setSocketDefaults(endPoint->get_fd()) < 0) {
        close(endPoint->get_fd());
        delete endPoint;
        return;
      }

      // Create the connection object and register our callbacks on various events.
      BaseConnection* conn = CreateConnection(endPoint, &connection_options_, eventLoop_);
      auto ccb = [conn, this](NetworkErrorCode ec) { this->OnConnectionClose(conn, ec); };
      conn->registerForClose(std::move(ccb));

      if (conn->start() != 0) {
        // Connection didn't start properly. Cleanup.
        // We assume here that this particular connection went bad, so we simply return.
        LOG(ERROR) << "Could not start the connection for read write";
        close(endPoint->get_fd());
        delete conn;
        return;
      }
      active_connections_.insert(conn);
      HandleNewConnection_Base(conn);
      return;
    } else {
      // accept failed.
      if (errno == EAGAIN) {
        // This is really odd. We thought that we had a read event
        LOG(ERROR) << "accept failed with EAGAIN when it should have worked. Ignoring";
      } else {
        LOG(ERROR) << "accept failed with errno " << errno;
      }
      close(endPoint->get_fd());
      delete endPoint;
      return;
    }
  } else {
    // What the hell, we only registered ourselves to reading
    // Just print a warning message
    LOG(WARNING) << "WARNING while expecting a read event we got " << _status;
    return;
  }
}

void BaseServer::OnConnectionClose(BaseConnection* _connection, NetworkErrorCode _status) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Connection closed for an unknown connection";
    _status = INVALID_CONNECTION;
  } else {
    active_connections_.erase(_connection);
  }
  HandleConnectionClose_Base(_connection, _status);
  delete _connection;
}

void BaseServer::CloseConnection_Base(BaseConnection* _connection) {
  InternalCloseConnection(_connection);
  return;
}

void BaseServer::InternalCloseConnection(BaseConnection* _connection) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Got the request close an unknown connection " << _connection << "\n";
    return;
  }
  _connection->closeConnection();
  return;
}

void BaseServer::AddTimer_Base(VCallback<> cb, sp_int64 _msecs) {
  InternalAddTimer(std::move(cb), _msecs);
}

void BaseServer::InternalAddTimer(VCallback<> cb, sp_int64 _msecs) {
  auto eCb = [cb, this](EventLoop::Status status) { this->OnTimer(std::move(cb), status); };

  CHECK_GT(eventLoop_->registerTimer(eCb, false, _msecs), 0);
}

void BaseServer::OnTimer(VCallback<> cb, EventLoop::Status) { cb(); }
