'''PyHeron's top level library, compatible with StreamParse API'''
__all__ = ['bolt', 'spout', 'stream', 'component', 'topology']

#########################################################################################
### Import modules that topology writers will frequently need to use/implement to     ###
### this top level, so that they can import them from the top level library directly. ###
#########################################################################################

# from heron's common module
from heron.common.src.python.utils.tuple import HeronTuple
from heron.common.src.python.utils.misc import PythonSerializer, IHeronSerializer
from heron.common.src.python.utils.topology import TopologyContext, ICustomGrouping, ITaskHook

import heron.common.src.python.constants as constants

# Load basic topology modules
from .stream import Stream, Grouping
from .topology import Topology, TopologyBuilder

# Load spout and bolt
from .bolt import Bolt
from .spout import Spout

###### Not yet implemented classes ######
class BatchingBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("BatchingBolt is not yet implemented in PyHeron")

class JavaBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("JavaBolt is not yet implemented in PyHeron")

class JavaSpout(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("JavaSpout is not yet implemented in PyHeron")

class ShellBolt(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("ShellBolt is not yet implemented in PyHeron")

class ShellSpout(object):
  def __init__(self, *args, **kwargs):
    raise NotImplementedError("ShellSpout is not yet implemented in PyHeron")

class StormHandler(object):
  def __init__(self, serializer):
    raise NotImplementedError("StormHandler is not yet implemented in PyHeron")
