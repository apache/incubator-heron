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

import collections

from heron.proto import topology_pb2

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

class GlobalStreamId(object):
  """Wrapper class to define stream_id and its component name

  Constructor method is compatible with StreamParse's GlobalStreamId class, although
  the object itself is completely different, as Heron does not use Thrift.
  This is mainly used for declaring input fields when defining a topology, and internally
  in HeronComponentSpec.
  """
  def __init__(self, componentId, streamId):
    """
    :type componentId: str
    :param componentId: component id from which the tuple is emitted
    :type streamId: str
    :param streamId: stream id through which the tuple is transmitted
    """
    if not isinstance(componentId, str) or not isinstance(streamId, str):
      raise ValueError('Both componentId and streamId must be string type')

    self.component_id = componentId
    self.stream_id = streamId

  def __eq__(self, other):
    return hasattr(other, 'component_id') and self.component_id == other.component_id \
           and hasattr(other, 'stream_id') and self.stream_id == other.stream_id

  def __hash__(self):
    return hash(self.__str__())

  def __str__(self):
    return self.component_id + ":" + self.stream_id

class Grouping(object):
  SHUFFLE = topology_pb2.Grouping.Value("SHUFFLE")
  ALL = topology_pb2.Grouping.Value("ALL")
  LOWEST = topology_pb2.Grouping.Value("LOWEST")
  NONE = topology_pb2.Grouping.Value("NONE")
  DIRECT = topology_pb2.Grouping.Value("DIRECT")
  CUSTOM = topology_pb2.Grouping.Value("CUSTOM")

  # gtype should contain topology_pb2.Grouping.Value("FIELDS")
  FIELDS = collections.namedtuple('FieldGrouping', 'gtype, fields')

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
    else:
      #TODO: DIRECT, CUSTOM are not supported yet
      return False

  @classmethod
  def fields(cls, *fields):
    if len(fields) == 1 and isinstance(fields[0], list):
      fields = fields[0]
    else:
      fields = list(fields)

    if not fields:
      raise ValueError("List cannot be empty for fields grouping")

    return cls.FIELDS(gtype=topology_pb2.Grouping.Value("FIELDS"),
                      fields=fields)
