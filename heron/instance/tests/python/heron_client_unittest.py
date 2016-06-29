# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from heron.instance.src.python.network.protocol import StatusCode
import heron.instance.src.python.network.mock_protobuf as mock_protobuf
import mock_generator

class ClientTest(unittest.TestCase):
  def setUp(self):
    self.mock_client = mock_generator.MockHeronClient()

  def test_handle_connect(self):
    self.mock_client.handle_connect()
    self.assertTrue(self.mock_client.passed_on_connect)

  def test_register_on_message(self):
    # try with pplan
    message = mock_protobuf.get_mock_pplan()
    fullname = message.DESCRIPTOR.full_name
    self.mock_client.register_on_message(mock_protobuf.get_pplan_builder_and_typename()[0])
    self.assertTrue(fullname in self.mock_client.registered_message_map)

  def test_handle_packet(self):
    # response -- status OK
    packet, reqid, message = mock_generator.get_a_mock_request_packet_and_raw()

    self.mock_client.context_map[reqid] = None
    self.mock_client.response_message_map[reqid] = message
    self.mock_client.handle_packet(packet)
    self.assertEqual(self.mock_client.on_response_status, StatusCode.OK)

    # response -- status INVALID_PACKET (parse invalid)
    self.mock_client.context_map[reqid] = None
    self.mock_client.response_message_map[reqid] = None
    self.mock_client.handle_packet(packet)
    self.assertEqual(self.mock_client.on_response_status, StatusCode.INVALID_PACKET)

    # message -- status OK
    pkt_list, msg_list, builder, typename = mock_generator.get_a_mock_message_list_and_builder()
    self.mock_client.registered_message_map[typename] = builder
    for pkt, msg in zip(pkt_list, msg_list):
      self.mock_client.handle_packet(pkt)
      self.assertEqual(self.mock_client.incoming_msg, msg)


  def test_handle_read(self):
    # normal
    self.mock_client.dispatcher.prepare_normal()
    self.mock_client.handle_read()
    self.assertTrue(self.mock_client.called_handle_packet)

    # header only
    self.mock_client.dispatcher.prepare_header_only()
    self.mock_client.handle_read()
    self.assertIsNotNone(self.mock_client.incomplete_pkt)
    self.assertTrue(self.mock_client.incomplete_pkt.is_header_read)
    self.assertFalse(self.mock_client.incomplete_pkt.is_complete)
