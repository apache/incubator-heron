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
from heron.instance.src.python.instance.comp_spec import HeronComponentSpec
from heron.instance.src.python.instance.stream import Stream

class TopologyType(type):
  """Metaclass to define a Heron topology in Python"""
  def __new__(mcs, classname, bases, class_dict):
    bolt_specs = {}
    spout_specs = {}
    # Copy HeronComponentSpec items out of class_dict
    specs = TopologyType.class_dict_to_specs(class_dict)
    for spec in specs.itervalues():
      if spec.is_spout:
        TopologyType.add_spout_specs(spec, spout_specs)
      else:
        TopologyType.add_bolt_specs(spec, bolt_specs)
    if classname != 'Topology' and not spout_specs:
      raise ValueError("A Topology requires at least one Spout")

    class_dict['protobuf_bolts'] = bolt_specs
    class_dict['protobuf_spouts'] = spout_specs
    class_dict['heron_specs'] = list(specs.values())

    # create topology protobuf here
    TopologyType.init_topology(classname, class_dict)

    return type.__new__(mcs, classname, bases, class_dict)

  @classmethod
  def class_dict_to_specs(mcs, class_dict):
    """Takes a class ``__dict__`` and returns the ``HeronComponentSpec`` entries"""
    specs = {}

    for name, spec in class_dict.iteritems():
      if isinstance(spec, HeronComponentSpec):
        # Use the variable name as the specification name.
          if spec.name is None:
            spec.name = name
          if spec.name in specs:
            raise ValueError("Duplicate component name: " + spec.name)
          else:
            specs[spec.name] = spec
    return specs

  @classmethod
  def add_spout_specs(mcs, spec, spout_specs):
    if not spec.outputs:
      raise ValueError(spec.python_class_path + " : " + spec.name +
                       " requires at least one output, because it is a spout")
    spout_specs[spec.name] = spec.get_protobuf()

  @classmethod
  def add_bolt_specs(mcs, spec, bolt_specs):
    if not spec.inputs:
      raise ValueError(spec.python_class_path + " : " + spec.name +
                       " requires at least one input, because it is a bolt")
    bolt_specs[spec.name] = spec.get_protobuf()

  @classmethod
  def get_default_topoconfig(mcs):
    config = topology_pb2.Config()
    conf_dict = {"topology.message.timeout.secs": "30",
                 "topology.acking": "false",
                 "topology.debug": "true"}

    for key, value in conf_dict.iteritems():
      kvs = config.kvs.add()
      kvs.key = key
      kvs.value = value
    return config

  @classmethod
  def init_topology(mcs, classname, class_dict):
    if classname == 'Topology':
      # Base class can't initialize protobuf
      return
    topology_name = classname + 'Topology'
    topology_id = topology_name + str(uuid.uuid4())

    # create protobuf
    topology = topology_pb2.Topology()
    topology.id = topology_id
    topology.name = topology_name
    topology.state = topology_pb2.TopologyState.Value("RUNNING")
    topology.topology_config.CopyFrom(TopologyType.get_default_topoconfig())

    TopologyType.add_bolts_and_spouts(topology, class_dict)

    class_dict['topology_name'] = topology_name
    class_dict['topology_id'] = topology_id
    class_dict['protobuf_topology'] = topology

  @classmethod
  def add_bolts_and_spouts(mcs, topology, class_dict):
    spouts = list(class_dict["protobuf_spouts"].values())
    bolts = list(class_dict["protobuf_bolts"].values())

    for spout in spouts:
      added = topology.spouts.add()
      added.CopyFrom(spout)
    for bolt in bolts:
      added = topology.bolts.add()
      added.CopyFrom(bolt)

class Topology(object):
  __metaclass__ = TopologyType
  @classmethod
  def write(cls, dest):
    if cls.__name__ == 'Topology':
      raise ValueError("The base Topology class cannot be writable")
    filename = cls.topology_name + ".defn"
    path = os.path.join(dest, filename)

    with open(path, 'wb') as f:
      f.write(cls.protobuf_topology.SerializeToString())
