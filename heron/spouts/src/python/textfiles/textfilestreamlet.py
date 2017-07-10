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

from heron.dsl.src.python import Streamlet, OperationType
from .textfilespout import TextFileSpout

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class TextFileStreamlet(Streamlet):
  """A TextFileStreamlet is a list of text input files
  """
  def __init__(self, filepattern, stage_name=None, parallelism=None):
    self._files = glob.glob(filepattern)
    super(TextFileStreamlet, self).__init__(operation=OperationType.Input,
                                            stage_name=stage_name,
                                            parallelism=parallelism)

  @staticmethod
  def textFile(filepattern, stage_name=None, parallelism=None):
    return TextFileStreamlet(filepattern, stage_name=stage_name, parallelism=parallelism)

  def _build(self, bldr, stage_names):
    self._parallelism = len(self._files)
    if self._parallelism < 1:
      raise RuntimeError("No matching files")
    if self._stage_name is None:
      index = 1
      self._stage_name = "textfileinput"
      while self._stage_name in stage_names:
        index = index + 1
        self._stage_name = "textfileinput" + str(index)
      bldr.add_spout(self._stage_name, TextFileSpout, par=self._parallelism,
                     config={TextFileSpout.FILES : self._files})
    return bldr
