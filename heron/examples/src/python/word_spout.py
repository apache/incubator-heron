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

from itertools import cycle
from heron.instance.src.python.instance.spout import Spout
from heron.common.src.python.color import Log


class WordSpout(Spout):
  def __init__(self, pplan_helper, in_stream, out_stream):
    super(WordSpout, self).__init__(pplan_helper, in_stream, out_stream)
    self.words = cycle(["hello", "bye", "good", "bad", "heron", "storm"])

  def open(self, config, context):
    Log.info("In open() of WordSpout")

  def next_tuple(self):
    word = next(self.words)
    self.emit([word])

  def fail(self, msg_id):
    pass

  def ack(self, msg_id):
    pass

