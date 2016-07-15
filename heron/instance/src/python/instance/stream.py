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


from heron.proto import topology_pb2

class Stream(object):
  DEFAULT_STREAM_ID = "default"


class Grouping(object):
  SHUFFLE = topology_pb2.Grouping.Value("SHUFFLE")
  FIELDS = topology_pb2.Grouping.Value("FIELDS")
  ALL = topology_pb2.Grouping.Value("ALL")
  LOWEST = topology_pb2.Grouping.Value("LOWEST")
  NONE = topology_pb2.Grouping.Value("NONE")
  DIRECT = topology_pb2.Grouping.Value("DIRECT")
  CUSTOM = topology_pb2.Grouping.Value("CUSTOM")

  # StreamParse compatibility
  GLOBAL = LOWEST
  LOCAL_OR_SHUFFLE = SHUFFLE

  @classmethod
  def fields(cls, *fields):
    if len(fields) == 1 and isinstance(fields[0], list):
      fields = fields[0]
    else:
      fields = list(fields)

    if not fields:
      raise ValueError("List cannot be empty for fields grouping")

    return cls.FIELDS, fields
