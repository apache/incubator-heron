#!/usr/bin/env python
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

'''aggregator_bolt.py'''
from future.standard_library import install_aliases
install_aliases()

import http.client
import json
from urllib.parse import urlparse

from heron.common.src.python.utils.log import Log
from ..core import constants as integ_constants
from .terminal_bolt import TerminalBolt

class AggregatorBolt(TerminalBolt):
  """Aggregator Bolt: aggregates the result of integration tests and posts it to the server"""
  # the last bolt has nothing to emit
  outputs = []

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    self.http_post_url = config[integ_constants.HTTP_POST_URL_KEY]
    self.result = []
    Log.info("HTTP post url: %s" % self.http_post_url)
    self.parsed_url = urlparse(self.http_post_url)

  def process(self, tup):
    self.result.append(tup.values[0])

  def _post_result_to_server(self, json_result):
    conn = http.client.HTTPConnection(self.parsed_url.netloc)
    conn.request("POST", self.parsed_url.path, json_result)
    response = conn.getresponse()
    if response.status == 200:
      Log.info("HTTP POST successful")
    else:
      Log.severe("HTTP POST failed, response code: %d, response: %s"
                 % (response.status, response.read()))
    return response.status

  def write_finished_data(self):
    json_result = json.dumps(self.result)
    Log.info("Actual result: %s" % json_result)
    Log.info("Posting actual result to %s" % self.http_post_url)
    try:
      response_code = self._post_result_to_server(json_result)
      if response_code != 200:
        # try again
        response_code = self._post_result_to_server(json_result)
        if response_code != 200:
          raise RuntimeError("Response code: %d" % response_code)
    except Exception as e:
      raise RuntimeError("Posting result to server failed with: %s" % e.message)
