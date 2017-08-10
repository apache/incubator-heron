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
'''fixedlinesstreamlet.py: module for defining a Streamlet based on FixedLinesSpout'''

from heron.dsl.src.python.streamlet import Streamlet
from heron.dsl.src.python.operation import OperationType
from heron.spouts.src.python.fixedlines.fixedlinesspout import FixedLinesSpout

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class FixedLinesStreamlet(Streamlet):
  """A FixedLinesStreamlet spews a set of words forever
  """
  # pylint: disable=no-self-use
  def __init__(self, stage_name=None, parallelism=None):
    super(FixedLinesStreamlet, self).__init__(parents=[], operation=OperationType.Input,
                                              stage_name=stage_name,
                                              parallelism=parallelism)

  @staticmethod
  def fixedLinesGenerator(stage_name=None, parallelism=None):
    return FixedLinesStreamlet(stage_name=stage_name, parallelism=parallelism)

  # pylint: disable=no-self-use
  def _calculate_inputs(self):
    return {}

  def _calculate_stage_name(self, existing_stage_names):
    stagename = "fixedlines"
    if stagename not in existing_stage_names:
      return stagename
    else:
      index = 1
      newname = stagename + str(index)
      while newname in existing_stage_names:
        index = index + 1
        newname = stagename + str(index)
      return newname

  def _build_this(self, bldr):
    bldr.add_spout(self._stage_name, FixedLinesSpout, par=self._parallelism)
