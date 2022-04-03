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


''' pidhandler.py '''
import subprocess
import tornado.web

class PidHandler(tornado.web.RequestHandler):
  """
  Responsible for getting the process ID for an instance.
  """

  # pylint: disable=attribute-defined-outside-init
  async def get(self, instance_id):
    ''' get method '''
    pid = subprocess.run(['cat', f"{instance_id}.pid"], capture_output=True, text=True,
                         check=True)
    await self.finish({
        'command': ' '.join(pid.args),
        'stdout': pid.stdout,
        'stderr': pid.stderr,
    })
