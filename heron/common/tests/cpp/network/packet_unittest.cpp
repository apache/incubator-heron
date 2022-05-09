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

#include "network/unittests.pb.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

// Test packet size for header
TEST(OutgoingPacketTest, test_header) {
  OutgoingPacket op(BUFSIZ);

  sp_uint32 explen = BUFSIZ + PacketHeader::header_size();
  EXPECT_EQ(explen, op.GetTotalPacketSize());
}

// Test packet size for integer
TEST(OutgoingPacketTest, test_int) {
  OutgoingPacket op(BUFSIZ);

  sp_int32 packint = 1234567890;
  op.PackInt(packint);

  sp_uint32 explen = PacketHeader::header_size() + sizeof(sp_int32);
  EXPECT_EQ(explen, op.GetBytesFilled());
}

// Test packet size for REQID
TEST(OutgoingPacketTest, test_reqid) {
  OutgoingPacket op(BUFSIZ);

  REQID_Generator gen;
  REQID packreqid = gen.generate();
  op.PackREQID(packreqid);

  sp_uint32 explen = PacketHeader::header_size() + REQID::length();
  EXPECT_EQ(explen, op.GetBytesFilled());
}

// Test packet size for string
TEST(OutgoingPacketTest, test_string) {
  OutgoingPacket op(BUFSIZ);

  sp_string str(10, 'a');
  op.PackString(str);

  sp_uint32 explen = PacketHeader::header_size() + sizeof(sp_uint32) + str.size();
  EXPECT_EQ(explen, op.GetBytesFilled());
}

// Test protobuf for message
TEST(OutgoingPacketTest, test_protobuf) {
  OutgoingPacket op(BUFSIZ);

  TestMessage tm;
  tm.add_message("abcdefghijklmnopqrstuvwxyz");
  op.PackProtocolBuffer(tm, tm.ByteSizeLong());

  sp_uint64 explen = PacketHeader::header_size() + sizeof(sp_uint32) + tm.ByteSizeLong();
  EXPECT_EQ(explen, op.GetBytesFilled());
}

// Test pack returns < 0 when the max packet size is
// exceeded for integers
TEST(OutgoingPacketTest, test_max_ints) {
  OutgoingPacket op(BUFSIZ);

  sp_int32 nints = BUFSIZ / sizeof(sp_int32);

  sp_int32 i = 0;
  for (; i < nints; i++) op.PackInt(i);

  EXPECT_LT(op.PackInt(++i), 0);
}

// Test pack returns < 0 when the max packet size is
// exceeded for several REQIDs
TEST(OutgoingPacketTest, test_max_reqids) {
  OutgoingPacket op(BUFSIZ);

  REQID_Generator gen;
  sp_int32 nids = BUFSIZ / REQID::length();

  REQID reqid;
  for (sp_int32 i = 0; i < nids; i++) {
    reqid = gen.generate();
    op.PackREQID(reqid);
  }

  EXPECT_LT(op.PackREQID(reqid), 0);
}

// Test pack with a mix of ints and strings
TEST(OutgoingPacketTest, test_variety) {
  OutgoingPacket op(64 * 1024);

  sp_string str(32, 'a');
  for (sp_int32 i = 0; i < 1024; i++) op.PackString(str);

  sp_uint32 explen = PacketHeader::header_size() + 1024 * (sizeof(sp_int32) + str.size());
  EXPECT_EQ(explen, op.GetBytesFilled());

  // Keep adding more
  for (sp_int32 i = 0; i < 1024; i++) op.PackInt(i);

  explen += 1024 * sizeof(sp_int32);
  EXPECT_EQ(explen, op.GetBytesFilled());
}

// Verify the correctness of an integer
TEST(IncomingPacketTest, test_int) {
  OutgoingPacket op(BUFSIZ);
  sp_int32 inta = 1234567890;

  op.PackInt(inta);

  IncomingPacket ip(op.get_header());
  sp_int32 intb;

  sp_uint32 explen = BUFSIZ + PacketHeader::header_size();
  //  PacketHeader::header_size() + sizeof(sp_int32);
  EXPECT_EQ(explen, ip.GetTotalPacketSize());

  ip.UnPackInt(&intb);
  EXPECT_EQ(inta, intb);
}

// Verify the correctness of a string
TEST(IncomingPacketTest, test_string) {
  OutgoingPacket op(BUFSIZ);

  sp_string str(32, 'a');
  op.PackString(str);

  IncomingPacket ip(op.get_header());

  sp_uint32 explen = BUFSIZ + PacketHeader::header_size();
  //  PacketHeader::header_size() + REQID::length();
  EXPECT_EQ(explen, ip.GetTotalPacketSize());

  sp_string istr;
  ip.UnPackString(&istr);
  EXPECT_EQ(str, istr);
}

// Verify the correctness of a reqid
TEST(IncomingPacketTest, test_reqid) {
  OutgoingPacket op(BUFSIZ);

  REQID_Generator gen;
  REQID reqida = gen.generate();
  op.PackREQID(reqida);

  IncomingPacket ip(op.get_header());
  REQID reqidb;

  sp_uint32 explen = BUFSIZ + PacketHeader::header_size();
  //  PacketHeader::header_size() + REQID::length();
  EXPECT_EQ(explen, ip.GetTotalPacketSize());

  ip.UnPackREQID(&reqidb);
  EXPECT_EQ(reqida, reqidb);
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
