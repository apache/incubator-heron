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
// Please see packet.h for details.
////////////////////////////////////////////////////////////////////////////////

#include "network/packet.h"
#include <arpa/inet.h>
#include <google/protobuf/message.h>
#include <string>
#include "glog/logging.h"
#include "network/regevent.h"

// PacketHeader static methods
void PacketHeader::set_packet_size(unsigned char* header, sp_uint32 _size) {
  sp_uint32 network_order = htonl(_size);
  memcpy(header, &network_order, sizeof(sp_uint32));
}

sp_uint32 PacketHeader::get_packet_size(const char* header) {
  sp_uint32 network_order = *(reinterpret_cast<const sp_uint32*>(header));
  return ntohl(network_order);
}

sp_uint32 PacketHeader::header_size() { return kSPPacketSize; }

// Constructor of the IncomingPacket. We only create the header buffer.
IncomingPacket::IncomingPacket(sp_uint32 _max_packet_size) {
  max_packet_size_ = _max_packet_size;
  position_ = 0;
  // bzero(header_, PacketHeader::size());
  data_ = NULL;
}

// Construct an incoming from a raw data buffer - used for tests only
IncomingPacket::IncomingPacket(unsigned char* _data) {
  memcpy(header_, _data, PacketHeader::header_size());
  data_ = new char[PacketHeader::get_packet_size(header_)];
  memcpy(data_, _data + PacketHeader::header_size(), PacketHeader::get_packet_size(header_));
  position_ = 0;
}

IncomingPacket::~IncomingPacket() { delete[] data_; }

sp_int32 IncomingPacket::UnPackInt(sp_int32* i) {
  if (data_ == NULL) return -1;
  if (position_ + sizeof(sp_int32) > PacketHeader::get_packet_size(header_)) return -1;
  sp_int32 network_order;
  memcpy(&network_order, data_ + position_, sizeof(sp_int32));
  position_ += sizeof(sp_int32);
  *i = ntohl(network_order);
  return 0;
}

sp_int32 IncomingPacket::UnPackString(sp_string* i) {
  sp_int32 size = 0;
  if (UnPackInt(&size) != 0) return -1;
  if (position_ + size > PacketHeader::get_packet_size(header_)) return -1;
  *i = std::string(data_ + position_, size);
  position_ += size;
  return 0;
}

sp_int32 IncomingPacket::UnPackProtocolBuffer(google::protobuf::Message* _proto) {
  sp_int32 sz;
  if (UnPackInt(&sz) != 0) return -1;
  if (position_ + sz > PacketHeader::get_packet_size(header_)) return -1;
  if (!_proto->ParsePartialFromArray(data_ + position_, sz)) return -1;
  position_ += sz;
  return 0;
}

sp_int32 IncomingPacket::UnPackREQID(REQID* _rid) {
  if (position_ + REQID_size > PacketHeader::get_packet_size(header_)) return -1;
  _rid->assign(std::string(data_ + position_, REQID_size));
  position_ += REQID_size;
  return 0;
}

sp_uint32 IncomingPacket::GetTotalPacketSize() const {
  return PacketHeader::get_packet_size(header_) + kSPPacketSize;
}

void IncomingPacket::Reset() { position_ = 0; }

sp_int32 IncomingPacket::Read(struct bufferevent* _buf) {
  if (data_ == NULL) {
    // We are still reading the header
    sp_int32 read_status =
        InternalRead(_buf, header_, PacketHeader::header_size());

    if (read_status != 0) {
      // Header read is either partial or had an error
      return read_status;
    } else {
      // Header just completed - some sanity checking of the header
      if (max_packet_size_ != 0 && PacketHeader::get_packet_size(header_) > max_packet_size_) {
        // Too large packet
        LOG(ERROR) << "Too large packet size " << PacketHeader::get_packet_size(header_)
                   << ". We only accept packet sizes <= " << max_packet_size_ << "\n";

        return -1;
      } else {
        // Create the data
        data_ = new char[PacketHeader::get_packet_size(header_)];

        // bzero(data_, PacketHeader::get_packet_size(header_));
        // reset the position to refer to the data_
      }
    }
  }

  // The header has been completely read. Read the data
  sp_int32 retval =
      InternalRead(_buf, data_, PacketHeader::get_packet_size(header_));
  if (retval == 0) {
    // Successfuly read the packet.
    position_ = 0;
  }

  return retval;
}

sp_int32 IncomingPacket::InternalRead(struct bufferevent* _buf, char* _buffer, sp_uint32 _size) {
  if (evbuffer_get_length(bufferevent_get_input(_buf)) < _size) {
    // We dont have all the data needed
    return 1;
  }
  int removed = evbuffer_remove(bufferevent_get_input(_buf), _buffer, _size);
  if (removed != _size) {
    LOG(ERROR) << "evbuffer remove failed. Expected to remove " << _size
               << " but removed only " << removed;
    return -1;
  }
  return 0;
}

OutgoingPacket::OutgoingPacket(sp_uint32 _packet_size) {
  total_packet_size_ = _packet_size + PacketHeader::header_size();
  buffer_ = evbuffer_new();
  struct evbuffer_iovec iovec;
  CHECK_EQ(evbuffer_reserve_space(buffer_, total_packet_size_, &iovec, 1), 1);
  underlying_data_ = (unsigned char*) iovec.iov_base;
  PacketHeader::set_packet_size(underlying_data_, _packet_size);
  position_ = PacketHeader::header_size();
}

OutgoingPacket::~OutgoingPacket() {
  if (buffer_) {
    evbuffer_free(buffer_);
  }
}

sp_uint32 OutgoingPacket::GetTotalPacketSize() const { return total_packet_size_; }

sp_uint32 OutgoingPacket::GetBytesFilled() const { return position_; }

sp_uint32 OutgoingPacket::GetBytesLeft() const { return total_packet_size_ - position_; }

sp_int32 OutgoingPacket::PackInt(const sp_int32& i) {
  if (sizeof(sp_int32) + position_ > total_packet_size_) {
    return -1;
  }
  sp_int32 network_order = htonl(i);
  memcpy(underlying_data_ + position_, &network_order, sizeof(sp_int32));
  position_ += sizeof(sp_int32);
  return 0;
}

sp_uint32 OutgoingPacket::SizeRequiredToPackProtocolBuffer(sp_int32 _byte_size) {
  return sizeof(sp_int32) + _byte_size;
}

sp_int32 OutgoingPacket::PackProtocolBuffer(const google::protobuf::Message& _proto,
                                            sp_int32 _byte_size) {
  if (PackInt(_byte_size) != 0) return -1;
  if (_byte_size + position_ > total_packet_size_) {
    return -1;
  }
  if (!_proto.SerializeWithCachedSizesToArray((underlying_data_ + position_))) {
    return -1;
  }
  position_ += _byte_size;
  return 0;
}

sp_int32 OutgoingPacket::PackProtocolBuffer(const char* _message,
                                            sp_int32 _byte_size) {
  if (PackInt(_byte_size) != 0) return -1;
  if (_byte_size + position_ > total_packet_size_) {
    return -1;
  }

  memcpy(underlying_data_ + position_, _message, _byte_size);
  position_ += _byte_size;
  return 0;
}

sp_int32 OutgoingPacket::PackREQID(const REQID& _rid) {
  if (REQID_size + position_ > total_packet_size_) {
    return -1;
  }
  memcpy(underlying_data_ + position_, _rid.c_str(), REQID_size);
  position_ += REQID_size;
  return 0;
}

sp_uint32 OutgoingPacket::SizeRequiredToPackString(const std::string& _input) {
  return sizeof(sp_uint32) + _input.size();
}

sp_int32 OutgoingPacket::PackString(const sp_string& i) {
  if (sizeof(sp_uint32) + i.size() + position_ > total_packet_size_) {
    return -1;
  }
  PackInt(i.size());
  memcpy(underlying_data_ + position_, i.c_str(), i.size());
  position_ += i.size();
  return 0;
}

struct evbuffer* OutgoingPacket::release_buffer() {
  struct evbuffer_iovec iovec;
  iovec.iov_base = underlying_data_;
  iovec.iov_len = total_packet_size_;
  CHECK_EQ(evbuffer_commit_space(buffer_, &iovec, 1), 0);
  auto retval = buffer_;
  buffer_ = NULL;
  underlying_data_ = NULL;
  position_ = 0;
  return retval;
}
