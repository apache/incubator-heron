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

'''arraylooper.py: module for defining a simple Generator'''

from collections.abc import Iterable
import itertools
import time

from heronpy.streamlet.generator import Generator

class ArrayLooper(Generator):
  """A ArrayLooper loops the contents of the a user supplied array forever
  """
  def __init__(self, user_iterable, sleep=None):
    super().__init__()
    if not isinstance(user_iterable, Iterable):
      raise RuntimeError("ArrayLooper must be passed an iterable")
    self._user_iterable = user_iterable
    self._sleep = sleep

  # pylint: disable=unused-argument, attribute-defined-outside-init
  def setup(self, context):
    self._curiter = itertools.cycle(self._user_iterable)

  def get(self):
    if self._sleep is not None:
      time.sleep(self._sleep)
    return next(self._curiter)
