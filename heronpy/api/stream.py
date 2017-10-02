# Copyright 2016 - Parsely, Inc. (d/b/a Parse.ly)
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
'''stream.py: module for defining Stream and Grouping for python topology'''

import collections

from heronpy.api.serializer import default_serializer
from heronpy.api.custom_grouping import ICustomGrouping
from heronpy.proto import topology_pb2

class Stream(object):
  """Heron output stream

  It is compatible with StreamParse API.
  """
  DEFAULT_STREAM_ID = "default"

  def __init__(self, fields=None, name=DEFAULT_STREAM_ID, direct=False):
    """
    :type fields: `list` or `tuple` of `str`
    :param fields: field names for this stream
    :type name: str
    :param name: name of stream. Defaults to ``default``
    :type direct: bool
    :param direct: whether or not this stream is direct. Default is ``False``
    """
    if fields is None:
      fields = []
    elif isinstance(fields, (list, tuple)):
      fields = list(fields)
      for field in fields:
        if not isinstance(field, str):
          raise TypeError("All field names must be strings, given: %s" % str(field))
    else:
      raise TypeError("Stream fields must be a list, tuple or None, given: %s" % str(fields))

    # self.fields is always list
    self.fields = fields

    if name is None:
      raise TypeError("Stream's name cannot be None")
    elif isinstance(name, str):
      self.stream_id = name
    else:
      raise TypeError("Stream name must be a string, given: %s" % str(name))

    if isinstance(direct, bool):
      self.direct = direct
      if self.direct:
        raise NotImplementedError("Direct stream is not supported yet.")
    else:
      raise TypeError("'direct' must be either True or False, given: %s" % str(direct))

class Grouping(object):
  """Helper class for defining Grouping for Python topology"""
  SHUFFLE = topology_pb2.Grouping.Value("SHUFFLE")
  ALL = topology_pb2.Grouping.Value("ALL")
  LOWEST = topology_pb2.Grouping.Value("LOWEST")
  NONE = topology_pb2.Grouping.Value("NONE")
  DIRECT = topology_pb2.Grouping.Value("DIRECT")

  # gtype should contain topology_pb2.Grouping.Value("FIELDS")
  FIELDS = collections.namedtuple('FieldGrouping', 'gtype, fields')

  # gtype should contain topology_pb2.Grouping.Value("CUSTOM")
  CUSTOM = collections.namedtuple('CustomGrouping', 'gtype, python_serialized')

  # StreamParse compatibility
  GLOBAL = LOWEST
  LOCAL_OR_SHUFFLE = SHUFFLE

  @classmethod
  def is_grouping_sane(cls, gtype):
    """Checks if a given gtype is sane"""
    if gtype == cls.SHUFFLE or gtype == cls.ALL or gtype == cls.LOWEST or gtype == cls.NONE:
      return True
    elif isinstance(gtype, cls.FIELDS):
      return gtype.gtype == topology_pb2.Grouping.Value("FIELDS") and \
             gtype.fields is not None
    elif isinstance(gtype, cls.CUSTOM):
      return gtype.gtype == topology_pb2.Grouping.Value("CUSTOM") and \
             gtype.python_serialized is not None
    else:
      #pylint: disable=fixme
      #TODO: DIRECT are not supported yet
      return False

  @classmethod
  def fields(cls, *fields):
    """Field grouping"""
    if len(fields) == 1 and isinstance(fields[0], list):
      fields = fields[0]
    else:
      fields = list(fields)

    for i in fields:
      if not isinstance(i, str):
        raise TypeError("Non-string cannot be specified in fields")

    if not fields:
      raise ValueError("List cannot be empty for fields grouping")

    return cls.FIELDS(gtype=topology_pb2.Grouping.Value("FIELDS"),
                      fields=fields)

  @classmethod
  def custom(cls, customgrouper):
    """Custom grouping from a given implementation of ICustomGrouping

    :param customgrouper: The ICustomGrouping implemention to use
    """
    if customgrouper is None:
      raise TypeError("Argument to custom() must be ICustomGrouping instance or classpath")
    if not isinstance(customgrouper, ICustomGrouping) and not isinstance(customgrouper, str):
      raise TypeError("Argument to custom() must be ICustomGrouping instance or classpath")
    serialized = default_serializer.serialize(customgrouper)
    return cls.custom_serialized(serialized, is_java=False)

  @classmethod
  def custom_serialized(cls, serialized, is_java=True):
    """Custom grouping from a given serialized string

    This class is created for compatibility with ``custom_serialized(cls, java_serialized)`` method
    of StreamParse API, although its functionality is not yet implemented (Java-serialized).
    Currently only custom grouping implemented in Python is supported, and ``custom()`` method
    should be used to indicate its classpath, rather than directly to use this method.

    In the future, users can directly specify Java-serialized object with ``is_java=True`` in order
    to use a custom grouping implemented in Java for python topology.

    :param serialized: serialized classpath to custom grouping class to use (if python)
    :param is_java: indicate whether this is Java serialized, or python serialized
    """
    if not isinstance(serialized, bytes):
      raise TypeError("Argument to custom_serialized() must be "
                      "a serialized Python class as bytes, given: %s" % str(serialized))
    if not is_java:
      return cls.CUSTOM(gtype=topology_pb2.Grouping.Value("CUSTOM"),
                        python_serialized=serialized)
    else:
      raise NotImplementedError("Custom grouping implemented in Java for Python topology"
                                "is not yet supported.")

  @classmethod
  def custom_object(cls, java_class_name, arg_list):
    """Tuples will be assigned to tasks by the given Java class."""
    raise NotImplementedError("custom_object() method is not yet implemented")
