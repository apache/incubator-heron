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
"""module for supplier spout: SupplierSpout"""
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.spout.spout import Spout

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletspoutbase import StreamletSpoutBase

# pylint: disable=unused-argument
class SupplierSpout(Spout, StatefulComponent, StreamletSpoutBase):
  """SupplierSpout"""
  FUNCTION = 'function'

  def init_state(self, stateful_state):
    # Supplier does not have any state
    pass

  def pre_save(self, checkpoint_id):
    # Supplier does not have any state
    pass

  def initialize(self, config, context):
    self.logger.debug("SupplierSpout's Component-specific config: \n%s" % str(config))
    self.emitted = 0
    if SupplierSpout.FUNCTION in config:
      self._supplier_function = config[SupplierSpout.FUNCTION]
    else:
      raise RuntimeError("SupplierSpout needs to be passed supplier function")

  def next_tuple(self):
    values = self._supplier_function()
    self.emit([values], stream='output')
    self.emitted += 1

# pylint: disable=protected-access
class SupplierStreamlet(Streamlet):
  """SupplierStreamlet"""
  def __init__(self, supplier_function):
    super(SupplierStreamlet, self).__init__()
    if not callable(supplier_function):
      raise RuntimeError("Supplier function has to be callable")
    self._supplier_function = supplier_function
    self.set_num_partitions(1)

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("supplier", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_spout(self.get_name(), SupplierSpout, par=self.get_num_partitions(),
                      config={SupplierSpout.FUNCTION : self._supplier_function})
    return True
