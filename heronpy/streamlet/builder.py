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
'''builder.py: module for creating streamlets'''

import sets

from heronpy.streamlet.generator import Generator
from heronpy.streamlet.impl.supplierspout import SupplierStreamlet
from heronpy.streamlet.impl.generatorspout import GeneratorStreamlet

class Builder(object):
  """A Builder object is used to build the functional api dag in Heron.
  """
  def __init__(self):
    """
    """
    self._sources = []

  def new_source(self, source):
    """Adds a new source to the computation dag"""

    source_streamlet = None
    if callable(source):
      source_streamlet = SupplierStreamlet(source)
    elif isinstance(source, Generator):
      source_streamlet = GeneratorStreamlet(source)
    else:
      raise RuntimeError("Builder's new source has to be either a Generator or a function")

    self._sources.append(source_streamlet)
    return source_streamlet

  # pylint: disable=protected-access
  def build(self, bldr):
    """Builds the topology and returns the builder"""
    stage_names = sets.Set()
    for source in self._sources:
      source._build(bldr, stage_names)
    for source in self._sources:
      if not source._all_built():
        raise RuntimeError("Topology cannot be fully built! Are all sources added?")
