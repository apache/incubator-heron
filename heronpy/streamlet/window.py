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

'''window.py: module for defining Window'''

class Window:
  """Window is a container containing information about a particular window.
     Transformations that depend on Windowing, pass the window information
     inside their streamlets using this container.
  """
  def __init__(self, start_time, end_time):
    self._start_time = start_time
    self._end_time = end_time

  def __repr__(self):
    return f'Window {{start_time: {self._start_time}, end_time: {self._end_time}}}'
