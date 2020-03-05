#!/usr/bin/env python
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""
topology.py: module for defining Heron topologies in Python
"""

import os
import uuid

import heronpy.api.api_constants as api_constants
import six
from heronpy.api.component.component_spec import HeronComponentSpec
from heronpy.api.serializer import default_serializer
from heronpy.proto import topology_pb2


class TopologyType(type):
  """Metaclass to define a Heron topology in Python"""
  DEFAULT_TOPOLOGY_CONFIG = {
      api_constants.TOPOLOGY_DEBUG: "false",
      api_constants.TOPOLOGY_STMGRS: "1",
      api_constants.TOPOLOGY_MESSAGE_TIMEOUT_SECS: "30",
      api_constants.TOPOLOGY_COMPONENT_PARALLELISM: "1",
      api_constants.TOPOLOGY_MAX_SPOUT_PENDING: "100",
      api_constants.TOPOLOGY_RELIABILITY_MODE: api_constants.TopologyReliabilityMode.ATMOST_ONCE,
      api_constants.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS: "true"}
  def __new__(mcs, classname, bases, class_dict):
    bolt_specs = {}
    spout_specs = {}
    # Copy HeronComponentSpec items out of class_dict
    specs = TopologyType.class_dict_to_specs(class_dict)
    for spec in iter(list(specs.values())):
      if spec.is_spout:
        TopologyType.add_spout_specs(spec, spout_specs)
      else:
        TopologyType.add_bolt_specs(spec, bolt_specs)
    if classname != 'Topology' and not spout_specs:
      raise ValueError("A Topology requires at least one Spout")

    topology_config = TopologyType.class_dict_to_topo_config(class_dict)

    if classname != 'Topology':
      class_dict['_topo_config'] = topology_config
      class_dict['_protobuf_bolts'] = bolt_specs
      class_dict['_protobuf_spouts'] = spout_specs
      class_dict['_heron_specs'] = list(specs.values())

      # create topology protobuf here
      TopologyType.init_topology(classname, class_dict)

    return type.__new__(mcs, classname, bases, class_dict)

  @classmethod
  def class_dict_to_specs(mcs, class_dict):
    """Takes a class `__dict__` and returns `HeronComponentSpec` entries"""
    specs = {}

    for name, spec in list(class_dict.items()):
      if isinstance(spec, HeronComponentSpec):
        # Use the variable name as the specification name.
        if spec.name is None:
          spec.name = name
        if spec.name in specs:
          raise ValueError("Duplicate component name: %s" % spec.name)
        else:
          specs[spec.name] = spec
    return specs

  @classmethod
  def class_dict_to_topo_config(mcs, class_dict):
    """
    Takes a class `__dict__` and returns a map containing topology-wide
    configuration.

    The returned dictionary is a sanitized `dict` of type `<str ->
    (str|object)>`.

    This classmethod firsts insert default topology configuration and then
    overrides it with a given topology-wide configuration. Note that this
    configuration will be overriden by a component-specific configuration at
    runtime.
    """
    topo_config = {}

    # add defaults
    topo_config.update(mcs.DEFAULT_TOPOLOGY_CONFIG)

    for name, custom_config in list(class_dict.items()):
      if name == 'config' and isinstance(custom_config, dict):
        sanitized_dict = mcs._sanitize_config(custom_config)
        topo_config.update(sanitized_dict)

    return topo_config

  @classmethod
  def add_spout_specs(mcs, spec, spout_specs):
    if not spec.outputs:
      raise ValueError(
          "%s: %s requires at least one output, because it is a spout" %
          (spec.python_class_path, spec.name))
    spout_specs[spec.name] = spec.get_protobuf()

  @classmethod
  def add_bolt_specs(mcs, spec, bolt_specs):
    if not spec.inputs:
      raise ValueError(
          "%s: %s requires at least one input, because it is a bolt" %
          (spec.python_class_path, spec.name))
    bolt_specs[spec.name] = spec.get_protobuf()

  @classmethod
  def get_topology_config_protobuf(mcs, class_dict):
    config = topology_pb2.Config()
    conf_dict = class_dict['_topo_config']

    for key, value in list(conf_dict.items()):
      if isinstance(value, str):
        kvs = config.kvs.add()
        kvs.key = key
        kvs.value = value
        kvs.type = topology_pb2.ConfigValueType.Value("STRING_VALUE")
      else:
        # need to serialize
        kvs = config.kvs.add()
        kvs.key = key
        kvs.serialized_value = default_serializer.serialize(value)
        kvs.type = topology_pb2.ConfigValueType.Value("PYTHON_SERIALIZED_VALUE")

    return config

  @classmethod
  def init_topology(mcs, classname, class_dict):
    """Initializes a topology protobuf"""
    if classname == 'Topology':
      # Base class can't initialize protobuf
      return
    heron_options = TopologyType.get_heron_options_from_env()
    initial_state = heron_options.get("cmdline.topology.initial.state", "RUNNING")
    tmp_directory = heron_options.get("cmdline.topologydefn.tmpdirectory")
    if tmp_directory is None:
      raise RuntimeError("Topology definition temp directory not specified")

    topology_name = heron_options.get("cmdline.topology.name", classname)
    topology_id = topology_name + str(uuid.uuid4())

    # create protobuf
    topology = topology_pb2.Topology()
    topology.id = topology_id
    topology.name = topology_name
    topology.state = topology_pb2.TopologyState.Value(initial_state)
    topology.topology_config.CopyFrom(TopologyType.get_topology_config_protobuf(class_dict))

    TopologyType.add_bolts_and_spouts(topology, class_dict)

    class_dict['topology_name'] = topology_name
    class_dict['topology_id'] = topology_id
    class_dict['protobuf_topology'] = topology
    class_dict['topologydefn_tmpdir'] = tmp_directory
    class_dict['heron_runtime_options'] = heron_options

  @staticmethod
  def get_heron_options_from_env():
    """Retrieves heron options from the `HERON_OPTIONS` environment variable.

    Heron options have the following format:

        cmdline.topologydefn.tmpdirectory=/var/folders/tmpdir
        cmdline.topology.initial.state=PAUSED

    In this case, the returned map will contain:

        #!json
        {
          "cmdline.topologydefn.tmpdirectory": "/var/folders/tmpdir",
          "cmdline.topology.initial.state": "PAUSED"
        }

    Currently supports the following options natively:

    - `cmdline.topologydefn.tmpdirectory`: (required) the directory to which this
    topology's defn file is written
    - `cmdline.topology.initial.state`: (default: "RUNNING") the initial state of the topology
    - `cmdline.topology.name`: (default: class name) topology name on deployment

    Returns: map mapping from key to value
    """
    heron_options_raw = os.environ.get("HERON_OPTIONS")
    if heron_options_raw is None:
      raise RuntimeError("HERON_OPTIONS environment variable not found")

    options = {}
    for option_line in heron_options_raw.replace("%%%%", " ").split(','):
      key, sep, value = option_line.partition("=")
      if sep:
        options[key] = value
      else:
        raise ValueError("Invalid HERON_OPTIONS part %r" % option_line)
    return options

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
  def _sanitize_config(custom_config):
    """Checks whether a given custom_config and returns a sanitized dict <str -> (str|object)>

    It checks if keys are all strings and sanitizes values of a given dictionary as follows:

    - If string, number or boolean is given as a value, it is converted to string.
      For string and number (int, float), it is converted to string by a built-in ``str()`` method.
      For a boolean value, ``True`` is converted to "true" instead of "True", and ``False`` is
      converted to "false" instead of "False", in order to keep the consistency with
      Java configuration.

    - If neither of the above is given as a value, it is inserted into the sanitized dict as it is.
      These values will need to be serialized before adding to a protobuf message.
    """
    sanitized = {}
    for key, value in list(custom_config.items()):
      if not isinstance(key, str):
        raise TypeError("Key for topology-wide configuration must be string, given: %s: %s"
                        % (str(type(key)), str(key)))

      if isinstance(value, bool):
        sanitized[key] = "true" if value else "false"
      elif isinstance(value, (str, int, float)):
        sanitized[key] = str(value)
      else:
        sanitized[key] = value

    return sanitized

@six.add_metaclass(TopologyType)
class Topology(object):
  """Topology is an abstract class for defining a topology

  Topology writers can define their custom topology by inheriting this class.
  The usage of this class is compatible with StreamParse API.

  Defining a topology is simple. Topology writers need to create a subclass, in which information
  about the components in their topology and how they connect to each other are specified
  by placing `HeronComponentSpec` as class instances.
  For more information, refer to `spec()` method of both the `Bolt` and `Spout` classes.

  In addition, you can also set a topology-wide configuration, by adding a ``config`` class
  attribute to your topology class, that is dict mapping from option names to their values.
  Note that topology-wide configurations are overridden by component-specific configurations
  that might be specified from ``spec()`` method of ``Bolt`` or ``Spout`` class.

  **Example**: A sample `WordCountTopology` can be defined as follows:

    #!python
    from heronpy.api.src.python import Topology
    from heronpy.examples.src.python import WordSpout, CountBolt

    class WordCount(Topology):
        config = {"topology.wide.config": "some value"}

        word_spout = WordSpout.spec(par=1)
        count_bolt = CountBolt.spec(par=1,
                                    inputs={word_spout: Grouping.fields('word')},
                                    config={"count_bolt.specific.config": "another value"})
  """

  # pylint: disable=no-member
  @classmethod
  def write(cls):
    """Writes the Topology .defn file to ``dest``

    This classmethod is meant be used by heron-cli when submitting a topology.
    """
    if cls.__name__ == 'Topology':
      raise ValueError("The base Topology class cannot be writable")
    filename = "%s.defn" % cls.topology_name
    path = os.path.join(cls.topologydefn_tmpdir, filename)

    with open(path, 'wb') as f:
      f.write(cls.protobuf_topology.SerializeToString())

class TopologyBuilder(object):
  """Builder for heronpy.api.src.python topology

  This class dynamically creates a subclass of `Topology` with given spouts and
  bolts and writes its definition files when `build_and_submit()` is called.

  **Example**: A sample `WordCountTopology` can be defined as follows:

    #!python
    import sys
    from heronpy.api.src.python import TopologyBuilder
    from heronpy.examples.spout import WordSpout
    from heronpy.examples.bolt import CountBolt

    if __name__ == '__main__':
        builder = TopologyBuilder(name=sys.argv[1])
        word_spout = builder.add_spout("word-spout", WordSpout, 2)

        builder.add_bolt("count-bolt", CountBolt, 2,
                         inputs={word_spout: Grouping.fields('word')},
                         config={"count_bolt.specific.config": "some value"})

        builder.build_and_submit()
  """
  def __init__(self, name):
    """Initialize this TopologyBuilder

    :type name: str
    :param name: topology name
    """
    assert name is not None and isinstance(name, str) and name != "Topology"

    self.topology_name = name

    self._specs = {}
    self._topology_config = {}

  def add_spec(self, *specs):
    """Add specs to the topology

    :type specs: HeronComponentSpec
    :param specs: specs to add to the topology
    """
    for spec in specs:
      if not isinstance(spec, HeronComponentSpec):
        raise TypeError("Argument to add_spec needs to be HeronComponentSpec, given: %s"
                        % str(spec))
      if spec.name is None:
        raise ValueError("TopologyBuilder cannot take a spec without name")
      if spec.name == "config":
        raise ValueError("config is a reserved name")
      if spec.name in self._specs:
        raise ValueError("Attempting to add duplicate spec name: %r %r" % (spec.name, spec))

      self._specs[spec.name] = spec

  def add_spout(self, name, spout_cls, par, config=None, optional_outputs=None):
    """Add a spout to the topology"""
    spout_spec = spout_cls.spec(name=name, par=par, config=config,
                                optional_outputs=optional_outputs)
    self.add_spec(spout_spec)
    return spout_spec

  def add_bolt(self, name, bolt_cls, par, inputs, config=None, optional_outputs=None):
    """Add a bolt to the topology"""
    bolt_spec = bolt_cls.spec(name=name, par=par, inputs=inputs, config=config,
                              optional_outputs=optional_outputs)
    self.add_spec(bolt_spec)
    return bolt_spec

  def set_config(self, config):
    """Set topology-wide configuration to the topology

    :type config: dict
    :param config: topology-wide config
    """
    if not isinstance(config, dict):
      raise TypeError("Argument to set_config needs to be dict, given: %s" % str(config))
    self._topology_config = config

  def _construct_topo_class_dict(self):
    class_dict = self._specs.copy()
    class_dict["config"] = self._topology_config
    return class_dict

  def build_and_submit(self):
    """Builds the topology and submits to the destination"""
    class_dict = self._construct_topo_class_dict()
    topo_cls = TopologyType(self.topology_name, (Topology,), class_dict)
    topo_cls.write()
