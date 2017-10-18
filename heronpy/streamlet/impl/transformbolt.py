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
"""module for map bolt: TransformBolt"""
from heronpy.api.bolt.bolt import Bolt
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.component.component_spec import GlobalStreamId
from heronpy.api.stream import Grouping

from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.transformoperator import TransformOperator
from heronpy.streamlet.impl.contextimpl import ContextImpl
from heronpy.streamlet.impl.streamletboltbase import StreamletBoltBase

# pylint: disable=unused-argument
class TransformBolt(Bolt, StatefulComponent, StreamletBoltBase):
  """TransformBolt"""
  OPERATOR = 'operator'

  # pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self._state = stateful_state

  def pre_save(self, checkpoint_id):
    # Nothing really
    pass

  def initialize(self, config, context):
    self.logger.debug("TransformBolt's Component-specific config: \n%s" % str(config))
    self.processed = 0
    self.emitted = 0
    if TransformBolt.OPERATOR in config:
      self._transform_operator = config[TransformBolt.OPERATOR]
    else:
      raise RuntimeError("TransformBolt needs to be passed transform_operator")
    if hasattr(self, '_state'):
      contextimpl = ContextImpl(context, self._state, self)
    else:
      contextimpl = ContextImpl(context, None, self)
    self._transform_operator.setup(contextimpl)

  def process(self, tup):
    self.transform_operator.transform(tup)
    self.processed += 1
    self.ack(tup)

# pylint: disable=protected-access
class TransformStreamlet(Streamlet):
  """TransformStreamlet"""
  def __init__(self, transform_operator, parent):
    super(TransformStreamlet, self).__init__()
    if not isinstance(transform_operator, TransformOperator):
      raise RuntimeError("Transform Operator has to be a TransformOperator")
    if not isinstance(parent, Streamlet):
      raise RuntimeError("parent of Transform Streamlet has to be a Streamlet")
    self._transform_operator = transform_operator
    self._parent = parent
    self.set_num_partitions(parent.get_num_partitions())

  def _calculate_inputs(self):
    return {GlobalStreamId(self._parent.get_name(), self._parent._output) :
            Grouping.SHUFFLE}

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("transform", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_bolt(self.get_name(), TransformBolt, par=self.get_num_partitions(),
                     inputs=self._calculate_inputs(),
                     config={TransformBolt.OPERATOR : self._transform_operator})
    return True
