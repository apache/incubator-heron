# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''resources.py: module for defining resources'''

class Resources(object):
  """Resources needed by the topology are encapsulated in this class.
     Currently we deal with cpu and ram. Others can be added later.
  """
  def __init__(self):
    self._cpu = 0.0
    self._ram = 0

  def set_cpu(self, cpu):
    self._cpu = float(cpu)
    return self

  def set_ram(self, ram):
    self._ram = int(ram)
    return self

  def get_cpu(self):
    return self._cpu

  def get_ram(self):
    return self._ram

  def set_ram_in_mb(self, ram):
    return self.set_ram(ram * 1024 * 1024)

  def set_ram_in_gb(self, ram):
    return self.set_ram_in_mb(ram * 1024)

  def __repr__(self):
    return 'Resource {cpu: %f, ram: %d}' % (self._cpu, self._ram)
