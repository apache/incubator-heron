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
'''base_spout.py'''
from ..component import HeronComponentSpec, BaseComponent
from ..stream import Stream

class BaseSpout(BaseComponent):
  """BaseSpout class

  This is the base for pyheron spout, which wraps the implementation of publicly available methods.
  This includes:
    - <classmethod> spec()
    - emit()

  They are compatible with StreamParse API.
  """
  # pylint: disable=no-member
  @classmethod
  def spec(cls, name=None, par=1, config=None):
    """Register this spout to the topology and create ``HeronComponentSpec``

    The usage of this method is compatible with StreamParse API, although it does not create
    ``ShellBoltSpec`` but instead directly registers to a ``Topology`` class.

    Note that this method does not take a ``outputs`` arguments because ``outputs`` should be
    an attribute of your ``Spout`` subclass.

    :type name: str
    :param name: Name of this spout.
    :type par: int
    :param par: Parallelism hint for this spout.
    :type config: dict
    :param config: Component-specific config settings.
    """
    python_class_path = "%s.%s" % (cls.__module__, cls.__name__)

    if hasattr(cls, 'outputs'):
      _outputs = cls.outputs
    else:
      _outputs = None

    return HeronComponentSpec(name, python_class_path, is_spout=True, par=par,
                              inputs=None, outputs=_outputs, config=config)

  # pylint: disable=unused-argument
  def emit(self, tup, tup_id=None, stream=Stream.DEFAULT_STREAM_ID,
           direct_task=None, need_task_ids=False):
    """Emits a new tuple from this Spout

    It is compatible with StreamParse API.

    :type tup: list or tuple
    :param tup: the new output Tuple to send from this spout,
                should contain only serializable data.
    :type tup_id: str or object
    :param tup_id: the ID for the Tuple. Leave this blank for an unreliable emit.
                   (Same as messageId in Java)
    :type stream: str
    :param stream: the ID of the stream this Tuple should be emitted to.
                   Leave empty to emit to the default stream.
    :type direct_task: int
    :param direct_task: the task to send the Tuple to if performing a direct emit.
    :type need_task_ids: bool
    :param need_task_ids: indicate whether or not you would like the task IDs the Tuple was emitted.
    """
    return self.delegate.emit(tup, tup_id, stream, direct_task, need_task_ids)
