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
// Implements the Server class. See server.h for details on the API
////////////////////////////////////////////////////////////////////////////////

#include "network/server.h"
#include <string>
#include <utility>
#include "basics/basics.h"

Server::Server(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options)
    : BaseServer(eventLoop, _options) {
  request_rid_gen_ = new REQID_Generator();
}

Server::~Server() {
  delete request_rid_gen_;
}

sp_int32 Server::Start() { return Start_Base(); }

sp_int32 Server::Stop() { return Stop_Base(); }

void Server::SendResponse(REQID _id, Connection* _connection,
                          const google::protobuf::Message& _response) {
  sp_int64 byte_size = _response.ByteSizeLong();
  sp_uint64 data_size = OutgoingPacket::SizeRequiredToPackString(_response.GetTypeName()) +
                        REQID_size + OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  auto opkt = new OutgoingPacket(data_size);
  CHECK_EQ(opkt->PackString(_response.GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(_id), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_response, byte_size), 0);
  InternalSendResponse(_connection, opkt);
  return;
}

void Server::SendMessage(Connection* _connection,
                         sp_int32 _byte_size,
                         const sp_string _type_name,
                         const char* _message) {
  // Generate a zero reqid
  REQID rid = REQID_Generator::generate_zero_reqid();

  sp_uint32 data_size = OutgoingPacket::SizeRequiredToPackString(_type_name) +
                          REQID_size + OutgoingPacket::SizeRequiredToPackProtocolBuffer(_byte_size);

  OutgoingPacket* opkt = new OutgoingPacket(data_size);

  CHECK_EQ(opkt->PackString(_type_name), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_message, _byte_size), 0);
  InternalSendResponse(_connection, opkt);
  return;
}

void Server::SendMessage(Connection* _connection, const google::protobuf::Message& _message) {
  // Generate a zero reqid
  REQID rid = REQID_Generator::generate_zero_reqid();
  // Currently its no different than response
  return SendResponse(rid, _connection, _message);
}

void Server::CloseConnection(Connection* _connection) { CloseConnection_Base(_connection); }

void Server::AddTimer(VCallback<> cb, sp_int64 _msecs) { AddTimer_Base(std::move(cb), _msecs); }

void Server::SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                         google::protobuf::Message* _response_placeholder) {
  SendRequest(_conn, _request, _ctx, -1, _response_placeholder);
}

void Server::SendRequest(Connection* _conn, google::protobuf::Message* _request, void* _ctx,
                         sp_int64 _msecs, google::protobuf::Message* _response_placeholder) {
  InternalSendRequest(_conn, _request, _msecs, _response_placeholder, _ctx);
}

// The interfaces of BaseServer being implemented
BaseConnection* Server::CreateConnection(ConnectionEndPoint* _endpoint, ConnectionOptions* _options,
                                         std::shared_ptr<EventLoop> eventLoop) {
  // Create the connection object and register our callbacks on various events.
  auto conn = new Connection(_endpoint, _options, eventLoop);
  auto npcb = [conn, this](IncomingPacket* packet) { this->OnNewPacket(conn, packet); };
  conn->registerForNewPacket(npcb);

  // Backpressure reliever - will point to the inheritor of this class in case the virtual function
  // is implemented in the inheritor
  auto backpressure_reliever_ = [this](Connection* conn) {
    this->StopBackPressureConnectionCb(conn);
  };

  auto backpressure_starter_ = [this](Connection* conn) {
    this->StartBackPressureConnectionCb(conn);
  };

  conn->registerForBackPressure(std::move(backpressure_starter_),
                                std::move(backpressure_reliever_));

  return conn;
}

void Server::HandleNewConnection_Base(BaseConnection* _connection) {
  HandleNewConnection(static_cast<Connection*>(_connection));
}

void Server::HandleConnectionClose_Base(BaseConnection* _connection, NetworkErrorCode _status) {
  HandleConnectionClose(static_cast<Connection*>(_connection), _status);
}

void Server::OnNewPacket(Connection* _connection, IncomingPacket* _packet) {
  // Maybe we can could the number of packets received by each connection?
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Packet Received on on unknown connection " << _connection << " from hostport "
               << _connection->getIPAddress() << ":" << _connection->getPort();
    delete _packet;
    _connection->closeConnection();
    return;
  }

  std::string typname;
  if (_packet->UnPackString(&typname) != 0) {
    LOG(ERROR) << "UnPackString failed from connection " << _connection << " from hostport "
               << _connection->getIPAddress() << ":" << _connection->getPort();
    delete _packet;
    _connection->closeConnection();
    return;
  }
  if (requestHandlers.count(typname) > 0) {
    // This is a request
    requestHandlers[typname](_connection, _packet);
  } else if (messageHandlers.count(typname) > 0) {
    // This is a message
    messageHandlers[typname](_connection, _packet);
  } else {
    // This might be a response for a send request
    REQID rid;
    CHECK_EQ(_packet->UnPackREQID(&rid), 0);
    if (context_map_.find(rid) != context_map_.end()) {
      // Yes this is indeed a good packet
      std::pair<google::protobuf::Message*, void*> pr = context_map_[rid];
      context_map_.erase(rid);
      NetworkErrorCode status = OK;
      if (_packet->UnPackProtocolBuffer(pr.first) != 0) {
        status = INVALID_PACKET;
      }
      auto cb = [pr, status, this]() { this->HandleResponse(pr.first, pr.second, status); };

      AddTimer(std::move(cb), 0);
    } else {
      // This is some unknown message
      LOG(ERROR) << "Unknown type protobuf received " << typname << " deleting...";
    }
  }
  delete _packet;
}

// Backpressure here - works for sending to both worker and stmgr
void Server::InternalSendResponse(Connection* _connection, OutgoingPacket* _packet) {
  if (active_connections_.find(_connection) == active_connections_.end()) {
    LOG(ERROR) << "Trying to send on unknown connection! Dropping.. " << std::endl;
    delete _packet;
    return;
  }
  if (_connection->sendPacket(_packet) != 0) {
    LOG(ERROR) << "Error sending packet to! Dropping... " << std::endl;
    delete _packet;
    return;
  }
  return;
}

void Server::InternalSendRequest(Connection* _conn, google::protobuf::Message* _request,
                                 sp_int64 _msecs, google::protobuf::Message* _response_placeholder,
                                 void* _ctx) {
  if (active_connections_.find(_conn) == active_connections_.end()) {
    delete _request;
    auto cb = [_response_placeholder, _ctx, this]() {
      this->HandleResponse(_response_placeholder, _ctx, WRITE_ERROR);
    };
    AddTimer(std::move(cb), 0);
    return;
  }

  // Generate the rid.
  REQID rid = request_rid_gen_->generate();

  // Insert into context map
  // TODO(kramasamy): If connection closes and there is no timeout associated with
  // a request, then the context_map_ will forever be left dangling.
  // One way to solve this would be to always have a timeout associated
  context_map_[rid] = std::make_pair(_response_placeholder, _ctx);

  // Make the outgoing packet
  sp_int64 byte_size = _request->ByteSizeLong();
  sp_uint64 sop = OutgoingPacket::SizeRequiredToPackString(_request->GetTypeName()) + REQID_size +
                  OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  auto opkt = new OutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_request->GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(*_request, byte_size), 0);

  // delete the request
  delete _request;

  if (_conn->sendPacket(opkt) != 0) {
    context_map_.erase(rid);
    delete opkt;
    auto cb = [_response_placeholder, _ctx, this]() {
      this->HandleResponse(_response_placeholder, _ctx, WRITE_ERROR);
    };
    AddTimer(std::move(cb), 0);
    return;
  }
  if (_msecs > 0) {
    auto cb = [rid, this](EventLoop::Status status) { this->OnPacketTimer(rid, status); };
    CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, _msecs), 0);
  }
  return;
}

void Server::OnPacketTimer(REQID _id, EventLoop::Status) {
  if (context_map_.find(_id) == context_map_.end()) {
    // most likely this was due to the requests being retired before the timer.
    return;
  }

  std::pair<google::protobuf::Message*, void*> pr = context_map_[_id];
  context_map_.erase(_id);

  HandleResponse(pr.first, pr.second, TIMEOUT);
}

// default implementation
void Server::HandleResponse(google::protobuf::Message* _response, void*, NetworkErrorCode) {
  delete _response;
}

void Server::StartBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}

void Server::StopBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}
