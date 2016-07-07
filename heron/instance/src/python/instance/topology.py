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
import os
import uuid
from heron.proto import topology_pb2

class Topology(object):
  DEFAULT_STREAM_ID = "default"

  def __init__(self, name):
    self.topology_name = name
    self.topology_id = name + str(uuid.uuid4())

    self.topology = topology_pb2.Topology()
    self.topology.id = self.topology_id
    self.topology.name = self.topology_name
    self.topology.state = topology_pb2.TopologyState.Value("RUNNING")

    self.spouts = {}
    self.bolts = {}

  def set_spout(self, name, spout_cls, classpath):
    """Set a spout

    Currently, only supports one output stream, with default stream id and "word" field
    """
    spout = topology_pb2.Spout()
    spout.comp.CopyFrom(self._get_base_component(name, classpath))

    # Just one output stream
    output_stream = spout.outputs.add()
    output_stream.CopyFrom(self._get_output_stream(name, self.DEFAULT_STREAM_ID, ["word"]))

    to_add = self.topology.spouts.add()
    to_add.CopyFrom(spout)
    self.spouts[name] = spout

  def set_bolt(self, name, bolt_cls, classpath):
    """Set a bolt

    Currently, everything that was emitted will be delivered to this bolt (All grouping).
    Just one input and output stream.
    """
    bolt = topology_pb2.Bolt()
    bolt.comp.CopyFrom(self._get_base_component(name, classpath))

    # input stream
    input_stream = bolt.inputs.add()
    input_stream.CopyFrom(self._get_input_stream(name, self.DEFAULT_STREAM_ID))

    # output stream
    output_stream = bolt.outputs.add()
    output_stream.CopyFrom(self._get_output_stream(name, self.DEFAULT_STREAM_ID, ["word"]))

    to_add = self.topology.bolts.add()
    to_add.CopyFrom(bolt)
    self.bolts[name] = bolt

  def write_to_file(self, dest):
    assert self.topology.IsInitialized()
    filename = self.topology_name + ".defn"
    path = os.path.join(dest, filename)

    with open(path, 'wb') as f:
      f.write(self.topology.SerializeToString())

  def _get_base_component(self, name, classpath, config=None):
    comp = topology_pb2.Component()
    comp.name = name
    comp.python_class_name = classpath
    if config is None:
      comp.config.CopyFrom(topology_pb2.Config())
    else:
      comp.config.CopyFrom(config)
    return comp

  def _get_output_stream(self, comp_name, stream_id, fields):
    output_stream = topology_pb2.OutputStream()
    output_stream.stream.CopyFrom(self._get_stream_id(comp_name, stream_id))
    output_stream.schema.CopyFrom(self._get_stream_schema(fields))
    return output_stream

  def _get_input_stream(self, comp_name, stream_id, gtype=topology_pb2.Grouping.Value("ALL")):
    input_stream = topology_pb2.InputStream()
    input_stream.stream.CopyFrom(self._get_stream_id(comp_name, stream_id))
    input_stream.gtype = gtype
    return input_stream

  def _get_stream_id(self, comp_name, id):
    stream_id = topology_pb2.StreamId()
    stream_id.id = id
    stream_id.component_name = comp_name
    return stream_id

  def _get_stream_schema(self, fields):
    stream_schema = topology_pb2.StreamSchema()
    for field in fields:
      key = stream_schema.keys.add()
      key.key = field
      key.type = topology_pb2.Type.Value("OBJECT")

    return stream_schema
