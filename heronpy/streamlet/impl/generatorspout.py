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
"""module for generator spout: GeneratorSpout"""
from heronpy.api.state.stateful_component import StatefulComponent
from heronpy.api.spout.spout import Spout

from heronpy.streamlet.impl.contextimpl import ContextImpl
from heronpy.streamlet.streamlet import Streamlet
from heronpy.streamlet.impl.streamletspoutbase import StreamletSpoutBase
from heronpy.streamlet.generator import Generator

# pylint: disable=unused-argument
class GeneratorSpout(Spout, StatefulComponent, StreamletSpoutBase):
  """GeneratorSpout"""
  GENERATOR = 'generator'

  #pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self._state = stateful_state

  def pre_save(self, checkpoint_id):
    pass

  def initialize(self, config, context):
    self.logger.debug("GeneratorSpout's Component-specific config: \n%s" % str(config))
    self.emitted = 0
    if GeneratorSpout.GENERATOR in config:
      self._generator = config[GeneratorSpout.GENERATOR]
    else:
      raise RuntimeError("GeneratorSpout needs to be passed generator function")
    if hasattr(self, '_state'):
      contextimpl = ContextImpl(context, self._state, self)
    else:
      contextimpl = ContextImpl(context, None, self)
    self._generator.setup(contextimpl)

  def next_tuple(self):
    values = self._generator.get()
    if values is not None:
      self.emit([values], stream='output')
      self.emitted += 1

# pylint: disable=protected-access
class GeneratorStreamlet(Streamlet):
  """GeneratorStreamlet"""
  def __init__(self, generator):
    super(GeneratorStreamlet, self).__init__()
    if not isinstance(generator, Generator):
      raise RuntimeError("Generator has to be of type Generator")
    self._generator = generator
    self.set_num_partitions(1)

  def _build_this(self, builder, stage_names):
    if not self.get_name():
      self.set_name(self._default_stage_name_calculator("generator", stage_names))
    if self.get_name() in stage_names:
      raise RuntimeError("Duplicate Names")
    stage_names.add(self.get_name())
    builder.add_spout(self.get_name(), GeneratorSpout, par=self.get_num_partitions(),
                      config={GeneratorSpout.GENERATOR : self._generator})
    return True
