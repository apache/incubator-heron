#!/usr/bin/env python
# -*- encoding: utf-8 -*-

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
''' pyobj.py '''
import sys

isPY3 = sys.version_info >= (3, 0, 0)

# helper method to support python 2 and 3
def is_str_instance(obj):
  if isPY3:
    return isinstance(obj, str)
  else:
    return str(type(obj)) == "<type 'unicode'>" or str(type(obj)) == "<type 'str'>"
