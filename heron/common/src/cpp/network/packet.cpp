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
// Please see packet.h for details.
////////////////////////////////////////////////////////////////////////////////

#include "network/packet.h"
#include <arpa/inet.h>
#include <google/protobuf/message.h>
#include <string>
#include "glog/logging.h"

// PacketHeader static methods
void PacketHeader::set_packet_size(char* header, sp_uint32 _size) {
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
IncomingPacket::IncomingPacket(char* _data) {
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

sp_int32 IncomingPacket::Read(sp_int32 _fd) {
  if (data_ == NULL) {
    // We are still reading the header
    sp_int32 read_status =
        InternalRead(_fd, header_ + position_, PacketHeader::header_size() - position_);

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

        position_ = 0;
      }
    }
  }

  // The header has been completely read. Read the data
  sp_int32 retval =
      InternalRead(_fd, data_ + position_, PacketHeader::get_packet_size(header_) - position_);
  if (retval == 0) {
    // Successfuly read the packet.
    position_ = 0;
  }

  return retval;
}

sp_int32 IncomingPacket::InternalRead(sp_int32 _fd, char* _buffer, sp_uint32 _size) {
  char* current = _buffer;
  sp_uint32 to_read = _size;
  while (to_read > 0) {
    ssize_t num_read = read(_fd, current, to_read);
    if (num_read > 0) {
      current = current + num_read;
      to_read = to_read - num_read;
      position_ = position_ + num_read;
    } else if (num_read == 0) {
      // remote end has done a shutdown.
      LOG(ERROR) << "Remote end has done a shutdown\n";
      return -1;
    } else {
      // read returned negative value.
      if (errno == EAGAIN) {
        // The read would block.
        // cout << "read syscall returned the EAGAIN errno " << errno << "\n";
        return 1;
      } else if (errno == EINTR) {
        // cout << "read syscall returned the EINTR errno " << errno << "\n";
        // the syscall encountered a signal before reading anything.
        // try again
        continue;
      } else {
        // something really bad happened. Bail out
        // try again
        LOG(ERROR) << "Something really bad happened while reading " << errno << "\n";
        return -1;
      }
    }
  }
  return 0;
}

OutgoingPacket::OutgoingPacket(sp_uint32 _packet_size) {
  total_packet_size_ = _packet_size + PacketHeader::header_size();
  data_ = new char[total_packet_size_];
  PacketHeader::set_packet_size(data_, _packet_size);
  position_ = PacketHeader::header_size();
}

OutgoingPacket::~OutgoingPacket() { delete[] data_; }

sp_uint32 OutgoingPacket::GetTotalPacketSize() const { return total_packet_size_; }

sp_uint32 OutgoingPacket::GetBytesFilled() const { return position_; }

sp_uint32 OutgoingPacket::GetBytesLeft() const { return total_packet_size_ - position_; }

sp_int32 OutgoingPacket::PackInt(const sp_int32& i) {
  if (sizeof(sp_int32) + position_ > total_packet_size_) {
    return -1;
  }
  sp_int32 network_order = htonl(i);
  memcpy(data_ + position_, &network_order, sizeof(sp_int32));
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
  if (!_proto.SerializeWithCachedSizesToArray((unsigned char*)(data_ + position_))) return -1;
  position_ += _byte_size;
  return 0;
}

sp_int32 OutgoingPacket::PackProtocolBuffer(const char* _message,
                                            sp_int32 _byte_size) {
  if (PackInt(_byte_size) != 0) return -1;
  if (_byte_size + position_ > total_packet_size_) {
    return -1;
  }

  memcpy(data_ + position_, _message, _byte_size);
  position_ += _byte_size;
  return 0;
}

sp_int32 OutgoingPacket::PackREQID(const REQID& _rid) {
  if (REQID_size + position_ > total_packet_size_) {
    return -1;
  }
  memcpy(data_ + position_, _rid.c_str(), REQID_size);
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
  memcpy(data_ + position_, i.c_str(), i.size());
  position_ += i.size();
  return 0;
}

void OutgoingPacket::PrepareForWriting() {
  CHECK(position_ == total_packet_size_);
  position_ = 0;
}

sp_int32 OutgoingPacket::Write(sp_int32 _fd) {
  while (position_ < total_packet_size_) {
    ssize_t num_written = write(_fd, data_ + position_, total_packet_size_ - position_);
    if (num_written > 0) {
      position_ = position_ + num_written;
    } else {
      if (errno == EAGAIN) {
        LOG(INFO) << "syscall write returned EAGAIN errno " << errno << "\n";
        return 1;
      } else if (errno == EINTR) {
        LOG(INFO) << "syscall write returned EINTR errno " << errno << "\n";
        continue;
      } else {
        LOG(ERROR) << "syscall write returned errno " << errno << "\n";
        return -1;
      }
    }
  }

  return 0;
}
