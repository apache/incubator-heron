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
'''textfilegenerator.py: module that defines a Heron Generator that reads data
   from a list of files and emits one tuple per line'''
import glob

from heronpy.streamlet.generator import Generator

class TextFileGenerator(Generator):
  """TextFileGenerator: reads from a list of files"""

  def __init__(self, filepattern):
    super(TextFileGenerator, self).__init__()
    self._files = glob.glob(filepattern)

  # pylint: disable=attribute-defined-outside-init
  def setup(self, context):
    """Implements TextFile Generator's setup method"""
    myindex = context.get_partition_index()
    self._files_to_consume = self._files[myindex::context.get_num_partitions()]
    self.logger.info("TextFileSpout files to consume %s" % self._files_to_consume)
    self._lines_to_consume = self._get_next_lines()
    self._emit_count = 0

  def get(self):
    if self._lines_to_consume is None:
      return None
    next_line = self._lines_to_consume.pop()
    if len(self._lines_to_consume) == 0:
      self._lines_to_consume = self._get_next_lines()
    self._emit_count += 1
    return next_line

  def _get_next_lines(self):
    next_lines = []
    while len(next_lines) == 0:
      next_lines = self._consume_next_file()
      if next_lines is None:
        return next_lines
    return next_lines

  def _consume_next_file(self):
    file_to_consume = self._get_next_file_to_consume()
    if file_to_consume is None:
      self.logger.info("All files consumed")
      return None
    self.logger.info("Now reading file %s" % file_to_consume)
    try:
      filep = open(file_to_consume, 'r')
      return filep.readlines()
    except IOError as e:
      self.logger.info("Could not open the file %s" % file_to_consume)
      raise e

  def _get_next_file_to_consume(self):
    if len(self._files_to_consume) == 0:
      return None
    return self._files_to_consume.pop()
