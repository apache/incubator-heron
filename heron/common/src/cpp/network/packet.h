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
// This file defines the IncomingPacket and OutgoingPacket classes.
// All communication between different heron servers occurs in Packets.
// Senders prepare an OutgoingPacket, pack it up with stuff they want to send
// and then send it. Recievers receive an IncomingPacket, unpack the stuff
// and use it.
//
// Heron Packets have the following format
// |-------------------------------------------------------------------------|
// |   Header        |                     Data                              |
// |-------------------------------------------------------------------------|
//
// The Header is a simple 4 byte unsigned int
//
////////////////////////////////////////////////////////////////////////////////
#ifndef PACKET_H_
#define PACKET_H_

#include <functional>
#include <string>
#include "basics/basics.h"

namespace google {
namespace protobuf {
class Message;
}
}

struct evbuffer;
struct bufferevent;

/*
 * The PacketHeader class defines a bunch of methods for the manipulation
 * of the Packet Header. Users pass the start of the header to its
 * functions. Always use PacketHeader class in dealing with a packet's
 * header instead of directly accessing the buffer.
 */

const sp_uint32 kSPPacketSize = sizeof(sp_uint32);

class PacketHeader {
 public:
  static void set_packet_size(unsigned char* header, sp_uint32 size);
  static sp_uint32 get_packet_size(const char* header);
  static sp_uint32 header_size();
};

/*
 * Class IncomingPacket - Definition of incoming packet
 *
 * Servers and clients use this structure to receive request/respones.
 * The underlying implementation uses two buffers. One for the header
 * and one for the actual data. We first read the header(since it is of
 * a fixed size. The header contain the data length information that allows
 * us to read the right amount of data. This implementation has the side
 * effect that we will need atleast two read calls to completely read a
 * packet.
 */
class IncomingPacket {
 public:
  //! Constructor
  // We will read a packet of maximum max_packet_size len. A value of zero
  // implies no limit
  explicit IncomingPacket(sp_uint32 max_packet_size);

  // Form an incoming packet with raw data buffer - used for tests only
  explicit IncomingPacket(unsigned char* _data);

  //! Destructor
  ~IncomingPacket();

  // UnPacking functions
  // Unpacking functions return a zero on successful operation. A negative value
  // implies error in unpacking. This could indicate a garbled packet

  // unpack an integer
  sp_int32 UnPackInt(sp_int32* i);

  // unpack a string
  sp_int32 UnPackString(sp_string* i);

  // unpack a protocol buffer
  sp_int32 UnPackProtocolBuffer(google::protobuf::Message* _proto);

  // unpack a request id
  sp_int32 UnPackREQID(REQID* _rid);

  // resets the read pointer to the start of the data.
  void Reset();

  // gets the header
  char* get_header() { return header_; }

  // Get the total size of the packet
  sp_uint32 GetTotalPacketSize() const;

 private:
  // Only Connection class can use the Read method to have
  // the packet read itself.
  friend class Connection;

  // Read the packet from the file descriptor fd.
  // Returns 0 if the packet has been read completely.
  // A > 0 return value indicates that the packet was
  // partially read and there was no more data. Further read
  // calls are necessary to completely read the packet.
  // A negative return value implies an irreovrable error
  sp_int32 Read(struct bufferevent* buf);

  // Helper method for Read to do the low level read calls.
  sp_int32 InternalRead(struct bufferevent* buf, char* buffer, sp_uint32 size);

  // The maximum packet length allowed. 0 means no limit
  sp_uint32 max_packet_size_;

  // The current read position.
  sp_uint32 position_;

  // The pointer to the header.
  char header_[kSPPacketSize];

  // The pointer to the data.
  char* data_;
};

/*
 * Class OutgoingPacket - Definition of outgoing packet
 *
 * The outgoing buffer is a contiguous stretch of data containing the header
 * and data. The current implementation mandates that user know the size of
 * the packet that they are sending.
 */
class OutgoingPacket {
 public:
  // Constructor/Destructors.
  // Constructor takes in a packet size parameter. The packet data
  // size must be exactly equal to the size specified.
  explicit OutgoingPacket(sp_uint32 packet_size);
  ~OutgoingPacket();

  // Packing functions
  // A zero return value indicates a successful operation.
  // A negative value implies packing error.

  // pack an integer
  sp_int32 PackInt(const sp_int32& i);

  // helper function to determine how much space is needed to encode a string
  static sp_uint32 SizeRequiredToPackString(const std::string& _input);

  // pack a string
  sp_int32 PackString(const sp_string& i);

  // helper function to determine how much space is needed to encode a protobuf
  // The paramter byte_size is the whats reported by the ByteSize
  static sp_uint32 SizeRequiredToPackProtocolBuffer(sp_int32 _byte_size);

  // pack a proto buffer
  sp_int32 PackProtocolBuffer(const google::protobuf::Message& _proto, sp_int32 _byte_size);

  sp_int32 PackProtocolBuffer(const char* _message, sp_int32 _byte_size);

  // pack a request id
  sp_int32 PackREQID(const REQID& _rid);

  // gets the header
  unsigned char* get_header() { return underlying_data_; }

  // gets the packet as a string
  const sp_string toRawData();

  // Get the total size of the packet
  sp_uint32 GetTotalPacketSize() const;

  // get the current length of the packet
  sp_uint32 GetBytesFilled() const;

  // get the number of bytes left
  sp_uint32 GetBytesLeft() const;

 private:
  // Only the Connection class can call the following functions
  friend class Connection;

  // This call releases the underlying evbuffer.
  // Only used by the connection class to send this packet
  struct evbuffer* release_buffer();

  // The current position of packing.
  sp_uint32 position_;

  // The actual data is held in this buffer
  struct evbuffer* buffer_;

  // This is the actual pointer to the data
  // that is inside the buffer_
  unsigned char* underlying_data_;

  // The packet size as specified in the constructor.
  sp_uint32 total_packet_size_;
};

#endif  // PACKET_H_
