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

import asyncore
import socket

from abc import abstractmethod
from utils import Log
from google.protobuf.message import Message

class HeronClient(asyncore.dispatcher):
  TIMEOUT_SEC = 30.0
  def __init__(self, hostname, port):
    asyncore.dispatcher.__init__(self)
    self.hostname = hostname
    self.port = int(port)
    self.out_buffer = ''
    self.in_buffer = ''
    # name to message.Message
    self.message_map = {}

  ##################################
  # asyncore.dispatcher override
  ##################################

  # called when connect is ready
  def handle_connect(self):
    Log.debug("Connected to " + self.hostname + ":" + str(self.port))
    self.on_connect()

  # called when close is ready
  def handle_close(self):
    self.close()

  # called when read is ready
  def handle_read(self):
    # TODO: read packets and pass them to handle_packet()
    pass

  def handle_write(self):
    sent = self.send(self.out_buffer)
    self.out_buffer = self.out_buffer[sent:]

  # called when an error was raised
  #def handle_error(self):
  #  Log.debug("Error occurred -- connection closed")
  #  self.stop()

  #################################

  def start(self):
    # TODO: specify buffer size, exception handling
    self.create_socket(socket.AF_INET, socket.SOCK_STREAM)

    # when ready, handle_connect is called
    self.connect((self.hostname, self.port))
    asyncore.loop(timeout=self.TIMEOUT_SEC)

  def stop(self):
    # TODO: cleanup things and close the connection
    self.handle_close()

  # Register the protobuf Message's name with protobuf Message
  def register_on_message(self, message):
    Log.debug("In register_on_message(): " + message.DESCRIPTOR.full_name)
    self.message_map[message.DESCRIPTOR.full_name] = message

  def send_request(self, request, context, response_type, timeout_sec):
    # TODO: send request and implement timeout handler
    Log.debug("In send_request(): \n" + request.__str__())
    self.out_buffer += request.__str__()

  def send_message(self):
    # TODO: sent message (non-request-response based communication)
    pass

  def handle_timeout(self):
    pass

  def handle_packet(self):
    # TODO: check if it has REQID: if yes, handle response message -- call on_reseponse()
    # otherwise, it's just an message -- call on_incoming_message()
    pass

  def get_classname(self):
    return self.__class__.__name__

  #######################################################
  # Below are the interfaces to be implemented by child #
  #######################################################

  @abstractmethod
  def on_connect(self):
    pass
