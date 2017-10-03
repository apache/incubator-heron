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
'''runner.py: module for creating Runner'''

from heronpy.api.topology import TopologyBuilder
from heronpy.dsl.impl.supplierspout import SupplierStreamlet
from heronpy.dsl.impl.generatorspout import GeneratorStreamlet

class Runner(object):
  """Runner is used to run a topology that is built by the builder.
     It exports a sole function called run that takes care of constructing the topology
  """
  def __init__(self):
    """
    """

  def run(self, name, config, builder):
    """
    """
    if not isinstance(name, str):
      raise RuntimeError("Name has to be a string type")
    if not isinstance(config, Config):
      raise RuntimeError("config has to be a Config type")
    if not isinstance(builder, Builder):
      raise RuntimeError("builder has to be a Builder type")
    bldr = builder.build(name)
    bldr.set_config(config._api_config)
    bldr.build_and_submit()
