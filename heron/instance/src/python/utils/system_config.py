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

''' sys_config '''
# pylint: disable=global-statement

from heron.common.src.python.utils import log

Log = log.Log
sys_config = {}

def merge(default, override):
  if isinstance(default, dict) and isinstance(override, dict):
    for k, v in list(override.items()):
      Log.info("Add overriding configuration '%s'", k)
      if k not in default:
        default[k] = v
      else:
        default[k] = merge(default[k], v)
  return default

def set_sys_config(default, override_config):
  global sys_config
  sys_config = merge(default, override_config)

def get_sys_config():
  return sys_config
