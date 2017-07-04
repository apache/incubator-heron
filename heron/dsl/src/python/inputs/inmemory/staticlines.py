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
'''staticlines.py: module for defining text file input'''
from heron.dsl.src.python import Streamlet, OperationType
from .staticlinesspout import StaticLinesSpout

class StaticLinesStreamlet(Streamlet):
  """A StaticLinesStreamlet spews a set of words forever
  """
  # pylint: disable=no-self-use
  def __init__(self, stage_name=None, parallelism=None):
    super(StaticLinesStreamlet, self).__init__(operation=OperationType.Input,
                                               stage_name=stage_name,
                                               parallelism=parallelism)

  @staticmethod
  def randomWordsGenerator(stage_name=None, parallelism=None):
    return StaticLinesStreamlet(stage_name=stage_name, parallelism=parallelism)

  def _build(self, bldr, stage_names):
    if Streamlet._parallelism is None:
      Streamlet._parallelism = 1
    if Streamlet._parallelism < 1:
      raise RuntimeError("StaticLines parallelism has to be >= 1")
    if Streamlet._stage_name is None:
      index = 1
      Streamlet._stage_name = "staticlines"
      while Streamlet._stage_name in stage_names:
        index = index + 1
        Streamlet._stage_name = "staticlines" + str(index)
      bldr.add_spout(Streamlet._stage_name, StaticLinesSpout, par=Streamlet._parallelism)
    return bldr
