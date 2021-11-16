#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


# pylint: disable=missing-docstring
import unittest
from heron.instance.src.python.network import REQID, HeronProtocol, IncomingPacket
import heron.instance.tests.python.network.mock_generator_client as mock_generator

class ProtocolTest(unittest.TestCase):
  def setUp(self):
    pass

  def test_reqid(self):
    reqid = REQID.generate()
    packed_reqid = REQID.pack(reqid)
    unpacked_reqid = REQID.unpack(packed_reqid)

    self.assertEqual(reqid, unpacked_reqid)
    self.assertFalse(reqid.is_zero())

    zero_reqid = REQID.generate_zero()
    packed_zero = REQID.pack(zero_reqid)
    # the length of REQID is 32 bytes
    self.assertEqual(packed_zero, bytearray(0 for i in range(32)))
    self.assertTrue(zero_reqid.is_zero())

  def test_encode_decode_packet(self):
    # get_mock_packets() uses OutgoingPacket.create_packet() to encode
    pkt_list, raw_list = mock_generator.get_mock_requst_packets(is_message=False)
    for pkt, raw in zip(pkt_list, raw_list):
      raw_reqid, raw_message = raw
      typename, reqid, seriazelid_msg = HeronProtocol.decode_packet(pkt)
      self.assertEqual(reqid, raw_reqid)
      self.assertEqual(typename, raw_message.DESCRIPTOR.full_name)
      self.assertEqual(seriazelid_msg, raw_message.SerializeToString())

  def test_fail_decode_packet(self):
    packet = mock_generator.get_fail_packet()
    with self.assertRaises(RuntimeError):
      HeronProtocol.decode_packet(packet)

  def test_read(self):
    # complete packets are prepared
    normal_dispatcher = mock_generator.MockDispatcher()
    normal_dispatcher.prepare_valid_response()
    pkt = IncomingPacket()
    pkt.read(normal_dispatcher)
    self.assertTrue(pkt.is_header_read)
    self.assertTrue(pkt.is_complete)

    # a packet with just a header is prepared
    header_dispatcher = mock_generator.MockDispatcher()
    header_dispatcher.prepare_header_only()
    pkt = IncomingPacket()
    pkt.read(header_dispatcher)
    self.assertTrue(pkt.is_header_read)
    self.assertFalse(pkt.is_complete)
    self.assertEqual(pkt.data, b"")

    # an incomplete data packet is prepared
    partial_data_dispatcher = mock_generator.MockDispatcher()
    partial_data_dispatcher.prepare_partial_data()
    pkt = IncomingPacket()
    pkt.read(partial_data_dispatcher)
    self.assertTrue(pkt.is_header_read)
    self.assertFalse(pkt.is_complete)
    self.assertEqual(len(pkt.data), partial_data_dispatcher.PARTIAL_DATA_SIZE)

    # error test
    try:
      eagain_dispatcher = mock_generator.MockDispatcher()
      eagain_dispatcher.prepare_eagain()
      pkt = IncomingPacket()
      pkt.read(eagain_dispatcher)
    except Exception:
      self.fail("Exception raised unexpectedly when testing EAGAIN error")

    with self.assertRaises(RuntimeError):
      fatal_dispatcher = mock_generator.MockDispatcher()
      fatal_dispatcher.prepare_fatal()
      pkt = IncomingPacket()
      pkt.read(fatal_dispatcher)
