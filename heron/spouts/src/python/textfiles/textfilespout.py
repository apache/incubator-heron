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
'''textfilespout.py: module that defines a Heron Spout that reads data
   from a list of files and emits one tuple per line'''
from heron.api.src.python.spout.spout import Spout

class TextFileSpout(Spout):
  """TextFileSpout: reads from a list of files"""

  # TopologyBuilder uses these constants to set config
  FILES = "files"

  def initialize(self, config, context):
    """Implements TextFile Spout's initialize method"""
    self.logger.info("Initializing TextFileSpout with the following")
    self.logger.info("Component-specific config: \n%s" % str(config))
    all_spout_tasks = context.get_component_tasks(context.get_component_id())
    all_spout_tasks.sort()
    if context.get_task_id() not in all_spout_tasks:
      raise RuntimeError("TextFileSpout's task_id %d not among all TextFileSpout %s" %
                         (context.get_task_id(), str(all_spout_tasks)))
    myindex = all_spout_tasks.index(context.get_task_id())
    if TextFileSpout.FILES not in config:
      raise RuntimeError("TextFileSpout's Files config not setup properly")
    all_files_to_consume = config[TextFileSpout.FILES]
    if not isinstance(all_files_to_consume, list):
      raise RuntimeError("TextFileSpout's Files config must be a list")
    self.files_to_consume = all_files_to_consume[myindex::len(all_spout_tasks)]
    self.logger.info("TextFileSpout files to consume %s" % self.files_to_consume)
    self.lines_to_consume = self._get_next_lines()
    self.emit_count = 0
    self.ack_count = 0
    self.fail_count = 0

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
    if len(self.files_to_consume) == 0:
      return None
    return self.files_to_consume.pop()

  def next_tuple(self):
    if self.lines_to_consume is None:
      return
    next_line = self.lines_to_consume.pop()
    if len(self.lines_to_consume) == 0:
      self.lines_to_consume = self._get_next_lines()
    self.emit(next_line)
    self.emit_count += 1

  def ack(self, tup_id):
    self.ack_count += 1
    self.logger.debug("Acked tuple %s" % str(tup_id))

  def fail(self, tup_id):
    self.fail_count += 1
    self.logger.debug("Failed tuple %s" % str(tup_id))
