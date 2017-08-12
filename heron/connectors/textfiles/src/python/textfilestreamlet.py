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
'''textfilestreamlet.py: module defining a streamlet based on TextFileSpout'''
import glob

from heron.dsl.src.python.streamlet import Streamlet
from heron.dsl.src.python.operation import OperationType
from heron.spouts.src.python.textfiles.textfilespout import TextFileSpout

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class TextFileStreamlet(Streamlet):
  """A TextFileStreamlet is a list of text input files
  """
  def __init__(self, filepattern, stage_name=None, parallelism=None):
    super(TextFileStreamlet, self).__init__(parents=[], operation=OperationType.Input,
                                            stage_name=stage_name,
                                            parallelism=parallelism)
    self._files = glob.glob(filepattern)

  @staticmethod
  def textFile(filepattern, stage_name=None, parallelism=None):
    return TextFileStreamlet(filepattern, stage_name=stage_name, parallelism=parallelism)

  # pylint: disable=no-self-use
  def _calculate_inputs(self):
    return {}

  def _calculate_parallelism(self):
    return len(self._files)

  # pylint: disable=no-self-use
  def _calculate_stage_name(self, existing_stage_names):
    funcname = "textfileinput"
    if funcname not in existing_stage_names:
      return funcname
    else:
      index = 1
      newfuncname = funcname + "-" + str(index)
      while newfuncname in existing_stage_names:
        index = index + 1
        newfuncname = funcname + str(index)
      return newfuncname

  def _build_this(self, bldr):
    bldr.add_spout(self._stage_name, TextFileSpout, par=self._parallelism,
                   config={TextFileSpout.FILES : self._files})
