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
from heron.instance.src.python.instance.component import HeronComponentSpec
from heron.instance.src.python.instance.stream import Stream

import heron.common.src.python.constants as constants

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

    topology_config = TopologyType.class_dict_to_topo_config(class_dict)

    class_dict['_topo_config'] = topology_config
    class_dict['_protobuf_bolts'] = bolt_specs
    class_dict['_protobuf_spouts'] = spout_specs
    class_dict['_heron_specs'] = list(specs.values())

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
  def class_dict_to_topo_config(mcs, class_dict):
    """Takes a class ``__dict__`` and returns a map containing topology-wide configuration

    This classmethod firsts insert default topology configuration, and then override them
    with a given topology-wide configuration.
    Note that this configuration will be overriden by a component-specific configuration at
    runtime.
    """
    topo_config = {}

    # add defaults
    default_topo_config = {constants.TOPOLOGY_DEBUG: "false",
                           constants.TOPOLOGY_STMGRS: "1",
                           constants.TOPOLOGY_MESSAGE_TIMEOUT_SECS: "30",
                           constants.TOPOLOGY_COMPONENT_PARALLELISM: "1",
                           constants.TOPOLOGY_MAX_SPOUT_PENDING: "100",
                           constants.TOPOLOGY_ENABLE_ACKING: "false",
                           constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: "true"}
    topo_config.update(default_topo_config)

    for name, custom_config in class_dict.iteritems():
      if name == 'config' and isinstance(custom_config, dict):
        sanitized_dict = mcs._sanitize_config(custom_config)
        topo_config.update(sanitized_dict)

    return topo_config

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
  def get_default_topoconfig(mcs, class_dict):
    config = topology_pb2.Config()
    conf_dict = class_dict['_topo_config']

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
    topology.topology_config.CopyFrom(TopologyType.get_default_topoconfig(class_dict))

    TopologyType.add_bolts_and_spouts(topology, class_dict)

    class_dict['topology_name'] = topology_name
    class_dict['topology_id'] = topology_id
    class_dict['protobuf_topology'] = topology

  @classmethod
  def add_bolts_and_spouts(mcs, topology, class_dict):
    spouts = list(class_dict["_protobuf_spouts"].values())
    bolts = list(class_dict["_protobuf_bolts"].values())

    for spout in spouts:
      added = topology.spouts.add()
      added.CopyFrom(spout)
    for bolt in bolts:
      added = topology.bolts.add()
      added.CopyFrom(bolt)

  @staticmethod
  def _sanitize_config(config_dict):
    """Checks if a given config_dict is sane, and returns a sanitized config_dict <str -> str>

    It checks if keys are all strings and convert all non-string values to strings
    """
    sanitized = {}
    for key, value in config_dict.iteritems():
      if not isinstance(key, str):
        raise TypeError("Key for topology-wide config must be string, given: " +
                        str(type(key)) + ":" + str(key))
      sanitized[key] = str(value)
    return sanitized


class Topology(object):
  """Topology is an abstract class to define a topology

  Topology writers can define their custom topology by inheriting this class.
  The usage of this class is compatible with StreamParse API.

  Defining a topology is simple. Topology writers need to create a subclass, in which information
  about the components in their topology and how they connect to each other are specified
  by placing ``HeronComponentSpec`` as class instances.
  For more information, refer to ``spec()`` method of both ``Bolt`` and ``Spout`` class.

  In addition to the compatibility with StreamParse API, this class supports ``config`` option,
  with which topology writers can specify topology-wide configurations. Note that topology-wide
  configurations are overridden by component-specific configurations that might be specified
  from ``spec()`` method of ``Bolt`` or ``Spout`` class. Specifying topology-wide configurations
  is also simple: topology writers need to declare an ``dict`` instance named ``config`` in their
  custom topology subclass.


  :Example: A sample WordCountTopology can be defined as follows:
  ::

    from pyheron import Topology
    from heron.examples.src.python import WordSpout, CountBolt

    class WordCount(Topology):
      config = {"topology.wide.config": "some value"}

      word_spout = WordSpout.spec(par=1)
      count_bolt = CountBolt.spec(par=1,
                                  inputs={word_spout: Grouping.fields('word')},
                                  config={"count_bolt.specific.config": "another value"})
  ::
  """
  __metaclass__ = TopologyType
  @classmethod
  def write(cls, dest):
    """Writes the Topology .defn file to ``dest``

    This classmethod is meant be used by heron-cli when submitting a topology.
    """
    if cls.__name__ == 'Topology':
      raise ValueError("The base Topology class cannot be writable")
    filename = cls.topology_name + ".defn"
    path = os.path.join(dest, filename)

    with open(path, 'wb') as f:
      f.write(cls.protobuf_topology.SerializeToString())
