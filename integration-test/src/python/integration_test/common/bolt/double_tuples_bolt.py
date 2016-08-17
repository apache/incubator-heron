# copyright 2016 twitter. all rights reserved.
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
'''double tuples bolt'''

from heron.pyheron.src.python import Bolt

class DoubleTuplesBolt(Bolt):
  """Double tuples bolt

  Output fields are not declared within this class, so they need to be
  provided from ``optional_outputs`` parameter in ``spec()``.
  """
  # output fields need to be set by topology
  def process(self, tup):
    self.emit(tup.values)
    self.emit(tup.values)
