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

import logging
from heron.proto import tuple_pb2, topology_pb2

from heron.common.src.python.utils.misc import PythonSerializer, OutgoingTupleHelper
from heron.instance.src.python.instance.stream import Stream, Grouping


class Component(object):
  """The base class for heron bolt/spout instance

  Implements the following functionality:
  1. Basic output collector API and pushing tuples to Out-Stream
  2. Run tasks continually

  :ivar pplan_helper: Physical Plan Helper for this component
  :ivar in_stream:    In-Stream Heron Communicator
  :ivar output_helper: Outgoing Tuple Helper
  :ivar serializer: Implementation of Heron Serializer
  """

  DEFAULT_STREAM_ID = "default"
  make_data_tuple = lambda _ : tuple_pb2.HeronDataTuple()

  def __init__(self, pplan_helper, in_stream, out_stream, looper, sys_config, serializer=PythonSerializer()):
    self.pplan_helper = pplan_helper
    self.in_stream = in_stream
    self.serializer = serializer
    self.output_helper = OutgoingTupleHelper(self.pplan_helper, out_stream)
    self.looper = looper
    self.sys_config = sys_config

    # will set a root logger here
    self.logger = logging.getLogger()

  @classmethod
  def get_python_class_path(cls):
    return cls.__module__ + "." + cls.__name__

  def log(self, message, level=None):
    """Log message, optionally providing a logging level

    It is compatible with StreamParse API.

    :type message: str
    :param message: the log message to send
    :type level: str
    :param level: the logging level, one of: trace (=debug), debug, info, warn or error (default: info)
    """
    if level is None:
      _log_level = logging.INFO
    else:
      if level == "trace" or level == "debug":
        _log_level = logging.DEBUG
      elif level == "info":
        _log_level = logging.INFO
      elif level == "warn":
        _log_level = logging.WARNING
      elif level == "error":
        _log_level = logging.ERROR
      else:
        raise ValueError(level + " is not supported as logging level")

    self.logger.log(_log_level, message)

  def admit_data_tuple(self, stream_id, data_tuple, tuple_size_in_bytes):
    self.output_helper.add_data_tuple(stream_id, data_tuple, tuple_size_in_bytes)

  def admit_control_tuple(self, control_tuple, tuple_size_in_bytes, is_ack):
    self.output_helper.add_control_tuple(control_tuple, tuple_size_in_bytes, is_ack)

  def get_total_data_emitted_in_bytes(self):
    return self.output_helper.total_data_emitted_in_bytes

  ##################################################################
  # The followings are to be implemented by Spout/Bolt independently
  ##################################################################

  def start(self):
    """Do the basic setup for Heron Instance"""
    raise NotImplementedError()

  def stop(self):
    """Do the basic clean for Heron Instance

    Note that this method is not guaranteed to be invoked
    """
    # TODO: We never actually call this method
    raise NotImplementedError()

  def process_incoming_tuples(self):
    """Should be called when a tuple was buffered into in_stream"""
    raise NotImplementedError()

  def _read_tuples_and_execute(self):
    """Read tuples from a queue and process the tuples"""
    raise NotImplementedError()

  def _activate(self):
    """Activate the instance"""
    raise NotImplementedError()

  def _deactivate(self):
    """Deactivate the instance"""
    raise NotImplementedError()

class HeronComponentSpec(object):
  def __init__(self, name, python_class_path, is_spout, par, inputs=None, outputs=None, config=None):
    self.name = name
    self.python_class_path = python_class_path
    self.is_spout = is_spout
    self.parallelism = par
    self.inputs = inputs
    self.outputs = outputs
    self.config = config

  def get_protobuf(self):
    """Returns protobuf object of this component"""
    if self.is_spout:
      return self._get_spout()
    else:
      return self._get_bolt()

  def _get_spout(self):
    spout = topology_pb2.Spout()
    spout.comp.CopyFrom(self._get_base_component())

    # Add output streams
    self._add_out_streams(spout)
    return spout

  def _get_bolt(self):
    bolt = topology_pb2.Bolt()
    bolt.comp.CopyFrom(self._get_base_component())

    # Add streams
    self._add_in_streams(bolt)
    self._add_out_streams(bolt)
    return bolt

  def _get_base_component(self):
    comp = topology_pb2.Component()
    comp.name = self.name
    comp.spec = topology_pb2.ComponentObjectSpec.Value("PYTHON_CLASS_NAME")
    comp.class_name = self.python_class_path
    comp.config.CopyFrom(self._get_comp_config())
    return comp

  def _get_comp_config(self):
    config = topology_pb2.Config()

    # first add parallelism
    key = config.kvs.add()
    key.key = "topology.component.parallelism"
    key.value = str(self.parallelism)

    # iterate through self.config
    if self.config is not None:
      for key, value in config.iteritems():
        kvs = config.kvs.add()
        kvs.key = key
        kvs.value = value
    return config

  def _add_in_streams(self, bolt):
    if self.inputs is None:
      return

    if isinstance(self.inputs, dict):
      # inputs are dictionary mapping from HeronComponentSpec to Grouping via default stream
      for comp_spec, gtype in self.inputs.iteritems():
        in_stream = bolt.inputs.add()
        in_stream.stream.CopyFrom(self._get_stream_id(comp_spec.name, Stream.DEFAULT_STREAM_ID))
        if isinstance(gtype, tuple) and gtype[0] == Grouping.FIELDS:
          # it's field grouping
          in_stream.gtype = Grouping.FIELDS
          grouping_fields = gtype[1]
          in_stream.grouping_fields.CopyFrom(self._get_stream_schema(grouping_fields))
        else:
          if gtype == Grouping.FIELDS or gtype == Grouping.CUSTOM or gtype == Grouping.DIRECT:
            raise ValueError("Grouping wrong")
          in_stream.gtype = gtype
    elif isinstance(self.inputs, list):
      # inputs are list of HeronComponentSpec or Stream
      # TODO: implement
      raise NotImplementedError("Input stream as list of HeronComponentSpec or Stream not supported yet")

  def _add_out_streams(self, spbl):
    # TODO: currently only supports default stream -- so just one output stream
    if self.outputs is None:
      return

    # Add default stream
    # Assuming that every element in outputs is output field, not Stream objects
    default_fields = [i for i in self.outputs if isinstance(i, str)]
    default_stream = spbl.outputs.add()
    default_stream.stream.CopyFrom(self._get_stream_id(self.name, Stream.DEFAULT_STREAM_ID))
    default_stream.schema.CopyFrom(self._get_stream_schema(default_fields))

  @staticmethod
  def _get_stream_id(comp_name, id):
    stream_id = topology_pb2.StreamId()
    stream_id.id = id
    stream_id.component_name = comp_name
    return stream_id

  @staticmethod
  def _get_stream_schema(fields):
    stream_schema = topology_pb2.StreamSchema()
    for field in fields:
      key = stream_schema.keys.add()
      key.key = field
      key.type = topology_pb2.Type.Value("OBJECT")

    return stream_schema

