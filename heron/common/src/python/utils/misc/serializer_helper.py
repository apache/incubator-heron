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
'''serializer_helper.py'''

from heron.common.src.python.utils.misc import PythonSerializer

import heron.common.src.python.constants as constants
import heron.common.src.python.pex_loader as pex_loader

class SerializerHelper(object):
  """Helper class for getting serializer for component"""
  @staticmethod
  def get_serializer(context):
    """Returns a serializer for a given context"""
    cluster_config = context.get_cluster_config()
    serializer_clsname = cluster_config.get(constants.TOPOLOGY_SERIALIZER_CLASSNAME, None)
    if serializer_clsname is None:
      return PythonSerializer()
    else:
      try:
        topo_pex_path = context.get_topology_pex_path()
        pex_loader.load_pex(topo_pex_path)
        serializer_cls = pex_loader.import_and_get_class(topo_pex_path, serializer_clsname)
        serializer = serializer_cls()
        return serializer
      except Exception as e:
        raise RuntimeError("Error with loading custom serializer class: %s, with error message: %s"
                           % (serializer_clsname, e.message))
