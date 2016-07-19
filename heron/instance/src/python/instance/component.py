import logging

from heron.proto import tuple_pb2

from heron.common.src.python.log import Log
from heron.instance.src.python.misc.outgoing_tuple_helper import OutgoingTupleHelper
from heron.instance.src.python.misc.serializer import PythonSerializer

# TODO: maybe implement some basic stuff


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

  def __init__(self, pplan_helper, in_stream, out_stream, looper, serializer=PythonSerializer()):
    self.pplan_helper = pplan_helper
    self.in_stream = in_stream
    self.serializer = serializer
    self.output_helper = OutgoingTupleHelper(self.pplan_helper, out_stream)
    self.looper = looper
    self.logger = Log

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

  def _admit_data_tuple(self, output_tuple, stream_id, is_spout, anchors=None, message_id=None):
    """Internal implementation of OutputCollector

    Handles emitting data tuples
    """
    # TODO (Important) : check whether this tuple is sane with pplan_helper.check_output_schema
    # TODO : custom grouping and invoke hook emit

    data_tuple = self.make_data_tuple()
    data_tuple.key = 0

    # TODO : set the anchors for a tuple (for Bolt), or message id (for Spout)

    tuple_size_in_bytes = 0

    # Serialize
    for object in output_tuple:
      serialized = self.serializer.serialize(object)
      data_tuple.values.append(serialized)
      tuple_size_in_bytes += len(serialized)

    self.output_helper.add_data_tuple(stream_id, data_tuple, tuple_size_in_bytes)


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

