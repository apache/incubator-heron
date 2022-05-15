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


''' jmaphandler.py '''
import json
import tornado.web

from heron.shell.src.python import utils

class JmapHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the jmap for a JVM process given its pid.
  """

  # pylint: disable=attribute-defined-outside-init
  async def get(self, pid):
    ''' get method '''
    utils.str_cmd(['rm', '-rf', '/tmp/heap.bin'], None, None)
    body = utils.str_cmd(['jmap', '-dump:format=b,file=/tmp/heap.bin',
                          str(pid)], None, None)
    utils.str_cmd(['chmod', '+r', '/tmp/heap.bin'], None, None)
    self.content_type = 'application/json'
    await self.finish(json.dumps(body))
