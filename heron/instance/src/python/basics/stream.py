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
'''stream.py: module for defining Stream and Grouping for python topology'''

import collections

from heron.proto import topology_pb2
from heron.common.src.python.utils.misc import default_serializer
from heron.common.src.python.utils.topology import ICustomGrouping

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
      raise TypeError("Straem fields must be a list, tuple or None, given: %s" % str(fields))

    # self.fields is always list
    self.fields = fields

    if isinstance(name, str):
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
  CUSTOM = topology_pb2.Grouping.Value("CUSTOM")

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
      #TODO: DIRECT, CUSTOM are not supported yet
      return False

  @classmethod
  def fields(cls, *fields):
    """Field grouping"""
    if len(fields) == 1 and isinstance(fields[0], list):
      fields = fields[0]
    else:
      fields = list(fields)

    if not fields:
      raise ValueError("List cannot be empty for fields grouping")

    return cls.FIELDS(gtype=topology_pb2.Grouping.Value("FIELDS"),
                      fields=fields)

  @classmethod
  def custom(cls, grouping):
    # TODO: serialize and return custom_serialized()
    if not isinstance(grouping, ICustomGrouping):
      raise TypeError("Argument to custom() must be an object of ICustomGrouping, given: "
                      "%s" % str(grouping))
    serialized = default_serializer.serialize(grouping)
    return cls.custom_serialized(serialized)

  @classmethod
  def custom_serialized(cls, python_serialized):
    """Custom grouping

    :param python_serialized: serialized Python object of CustomGroupingStream
    """
    if not isinstance(python_serialized, bytes):
      raise TypeError("Argument to custom_serialized() must be "
                      "a serialized Python class as bytes, given: %s" % str(python_serialized))
    return cls.CUSTOM(gtype=topology_pb2.Grouping.Value("CUSTOM"),
                      python_serialized=python_serialized)

