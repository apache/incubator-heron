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
// Implements the Client class. See client.h for details on the API
////////////////////////////////////////////////////////////////////////////////
#include "network/client.h"
#include <string>
#include "basics/basics.h"

Client::Client(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options)
    : BaseClient(eventLoop, _options) {
  Init();
}

Client::~Client() {
  delete message_rid_gen_;
}

void Client::Start() { Start_Base(); }

void Client::Stop() { Stop_Base(); }

void Client::SendRequest(std::unique_ptr<google::protobuf::Message> _request, void* _ctx) {
  SendRequest(std::move(_request), _ctx, -1);
}

void Client::SendRequest(std::unique_ptr<google::protobuf::Message> _request, void* _ctx,
        sp_int64 _msecs) {
  InternalSendRequest(std::move(_request), _ctx, _msecs);
}

void Client::SendResponse(REQID _id, const google::protobuf::Message& _response) {
  sp_int64 byte_size = _response.ByteSizeLong();
  sp_uint64 data_size = OutgoingPacket::SizeRequiredToPackString(_response.GetTypeName()) +
                        REQID_size + OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  auto opkt = new OutgoingPacket(data_size);
  CHECK_EQ(opkt->PackString(_response.GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(_id), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_response, byte_size), 0);
  InternalSendResponse(opkt);
  return;
}

void Client::SendMessage(const google::protobuf::Message& _message) {
  InternalSendMessage(_message);
}

sp_int64 Client::AddTimer(VCallback<> cb, sp_int64 _msecs) {
  return AddTimer_Base(std::move(cb), _msecs);
}

sp_int32 Client::RemoveTimer(sp_int64 timer_id) { return RemoveTimer_Base(timer_id); }

BaseConnection* Client::CreateConnection(ConnectionEndPoint* _endpoint, ConnectionOptions* _options,
                                         std::shared_ptr<EventLoop> eventLoop) {
  auto conn = new Connection(_endpoint, _options, eventLoop);

  conn->registerForNewPacket([this](IncomingPacket* pkt) { this->OnNewPacket(pkt); });
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

void Client::HandleConnect_Base(NetworkErrorCode _status) { HandleConnect(_status); }

void Client::HandleClose_Base(NetworkErrorCode _status) { HandleClose(_status); }

void Client::Init() { message_rid_gen_ = new REQID_Generator(); }

void Client::InternalSendRequest(std::unique_ptr<google::protobuf::Message> _request, void* _ctx,
        sp_int64 _msecs) {
  auto iter = requestResponseMap_.find(_request->GetTypeName());
  CHECK(iter != requestResponseMap_.end());
  const sp_string& _expected_response_type = iter->second;
  if (state_ != CONNECTED) {
    responseHandlers[_expected_response_type](NULL, WRITE_ERROR);
    return;
  }

  // Generate the rid.
  REQID rid = message_rid_gen_->generate();

  // Insert into context map
  context_map_[rid] = std::make_pair(_expected_response_type, _ctx);

  // Make the outgoing packet
  sp_int64 byte_size = _request->ByteSizeLong();
  sp_uint64 sop = OutgoingPacket::SizeRequiredToPackString(_request->GetTypeName()) + REQID_size +
                  OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  auto opkt = new OutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_request->GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(*_request, byte_size), 0);

  Connection* conn = static_cast<Connection*>(conn_);
  if (conn->sendPacket(opkt) != 0) {
    context_map_.erase(rid);
    delete opkt;
    responseHandlers[_expected_response_type](NULL, WRITE_ERROR);
    return;
  }
  if (_msecs > 0) {
    auto cb = [rid, this](EventLoop::Status s) { this->OnPacketTimer(rid, s); };
    CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, _msecs), 0);
  }
  return;
}

void Client::InternalSendMessage(const google::protobuf::Message& _message) {
  if (state_ != CONNECTED) {
    LOG(ERROR) << "Client is not connected. Dropping message" << std::endl;

    return;
  }

  // Generate a zero rid.
  REQID rid = REQID_Generator::generate_zero_reqid();

  // Make the outgoing packet
  sp_int64 byte_size = _message.ByteSizeLong();
  sp_uint64 sop = OutgoingPacket::SizeRequiredToPackString(_message.GetTypeName()) + REQID_size +
                  OutgoingPacket::SizeRequiredToPackProtocolBuffer(byte_size);
  auto opkt = new OutgoingPacket(sop);
  CHECK_EQ(opkt->PackString(_message.GetTypeName()), 0);
  CHECK_EQ(opkt->PackREQID(rid), 0);
  CHECK_EQ(opkt->PackProtocolBuffer(_message, byte_size), 0);

  Connection* conn = static_cast<Connection*>(conn_);
  if (conn->sendPacket(opkt) != 0) {
    LOG(ERROR) << "Some problem sending message thru the connection. Dropping message" << std::endl;
    delete opkt;
    return;
  }
  return;
}

void Client::InternalSendResponse(OutgoingPacket* _packet) {
  if (state_ != CONNECTED) {
    LOG(ERROR) << "Client is not connected. Dropping response" << std::endl;
    delete _packet;
    return;
  }

  Connection* conn = static_cast<Connection*>(conn_);
  if (conn->sendPacket(_packet) != 0) {
    LOG(ERROR) << "Error sending packet to! Dropping..." << std::endl;
    delete _packet;
    return;
  }
  return;
}

void Client::OnNewPacket(IncomingPacket* _ipkt) {
  std::string typname;

  if (_ipkt->UnPackString(&typname) != 0) {
    Connection* conn = static_cast<Connection*>(conn_);
    LOG(FATAL) << "UnPackString failed from connection " << conn << " from hostport "
               << conn->getIPAddress() << ":" << conn->getPort();
  }

  if (messageHandlers.count(typname) > 0) {
    // This is a message
    // We just ignore the reqid
    REQID rid;
    CHECK_EQ(_ipkt->UnPackREQID(&rid), 0);
    // This is a message
    messageHandlers[typname](_ipkt);
  } else if (responseHandlers.count(typname) > 0) {
    // This is a response
    responseHandlers[typname](_ipkt, OK);
  } else {
    LOG(ERROR) << "Got a packet type that is not registered " << typname;
  }
  delete _ipkt;
}

void Client::OnPacketTimer(REQID _id, EventLoop::Status) {
  if (context_map_.find(_id) == context_map_.end()) {
    // most likely this was due to the requests being retired before the timer.
    return;
  }

  const sp_string& expected_response_type = context_map_[_id].first;
  responseHandlers[expected_response_type](NULL, TIMEOUT);
}

void Client::StartBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}

void Client::StopBackPressureConnectionCb(Connection* conn) {
  // Nothing to be done here. Should be handled by inheritors if they care about backpressure
}

bool Client::HasCausedBackPressure() const {
  Connection* conn = static_cast<Connection*>(conn_);
  return conn && conn->hasCausedBackPressure();
}
