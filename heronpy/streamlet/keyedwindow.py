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

'''keyedwindow.py: module for defining KeyedWindow'''

from heronpy.streamlet.window import Window

class KeyedWindow:
  """Transformation depending on Windowing pass on the window/key information
     using this class
  """
  def __init__(self, key, window):
    if not isinstance(window, Window):
      raise RuntimeError("Window of KeyedWindow has to be of type Window")
    self._key = key
    self._window = window

  def __repr__(self):
    return f'KeyedWindow {{key: {self._key}, window: {self._window}}}'
