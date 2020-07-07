#!/usr/bin/env python3
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


# pylint: disable=missing-docstring
# pylint: disable=protected-access
import os
import unittest

from heronpy.api.serializer import default_serializer
from heronpy.api.topology import Topology, TopologyBuilder, TopologyType
from heronpy.api.stream import Stream, Grouping
from heronpy.api.component.component_spec import HeronComponentSpec
from heronpy.proto import topology_pb2

# required environment variable
# note that this test doesn't write anything to /tmp directory
heron_options = "cmdline.topologydefn.tmpdirectory=/tmp,cmdline.topology.initial.state=RUNNING"
os.environ["HERON_OPTIONS"] = heron_options

class TestSane(Topology):
  config = {"topology.wide.config.1": "value",
            "spout.overriden.config": True}
  spout = HeronComponentSpec(None, "sp_class", True, 3, inputs=None,
                             outputs=["word", "count",
                                      Stream(fields=['error_msg'], name='error_stream')],
                             config={"spout.specific.config.1": "value",
                                     "spout.specific.config.2": True,
                                     "spout.specific.config.3": -12.4,
                                     "spout.specific.config.4": [1, 2, 3],
                                     "spout.overriden.config": False})
  bolt = HeronComponentSpec(None, "bl_class", False, 4,
                            inputs={spout: Grouping.SHUFFLE, spout['error_stream']: Grouping.ALL})

# pylint: disable=no-member
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
class TopologyTest(unittest.TestCase):
  def setUp(self):
    os.environ["HERON_OPTIONS"] = heron_options

  def tearDown(self):
    os.environ.pop("HERON_OPTIONS", None)

  def test_sane_topology(self):
    self.assertEqual(TestSane.topology_name, "TestSane")

    # topology-wide config
    expecting_topo_config = TopologyType.DEFAULT_TOPOLOGY_CONFIG
    expecting_topo_config.update({"topology.wide.config.1": "value",
                                  "spout.overriden.config": "true"})
    self.assertEqual(TestSane._topo_config, expecting_topo_config)

    self.assertEqual(len(TestSane._protobuf_bolts), 1)
    self.assertEqual(len(TestSane._protobuf_spouts), 1)

    self.assertEqual(len(TestSane._heron_specs), 2)
    for spec in TestSane._heron_specs:
      if spec.is_spout:
        self.assertEqual(spec.name, "spout")
        self.assertEqual(spec.python_class_path, "sp_class")
        self.assertEqual(spec.parallelism, 3)
      else:
        self.assertEqual(spec.name, "bolt")
        self.assertEqual(spec.python_class_path, "bl_class")
        self.assertEqual(spec.parallelism, 4)

    self.assertTrue(isinstance(TestSane.protobuf_topology, topology_pb2.Topology))

    proto_topo = TestSane.protobuf_topology

    ### spout protobuf ###
    self.assertEqual(len(proto_topo.spouts), 1)
    spout = proto_topo.spouts[0]
    self.assertEqual(spout.comp.name, "spout")
    self.assertEqual(spout.comp.spec, topology_pb2.ComponentObjectSpec.Value("PYTHON_CLASS_NAME"))
    self.assertEqual(spout.comp.class_name, "sp_class")
    expecting_spout_config = {"topology.component.parallelism": "3",
                              "spout.specific.config.1": "value",
                              "spout.specific.config.2": "true",
                              "spout.specific.config.3": "-12.4",
                              "spout.specific.config.4": default_serializer.serialize([1, 2, 3]),
                              "spout.overriden.config": "false"}
    self.assertEqual(len(spout.comp.config.kvs), len(expecting_spout_config))
    for conf in spout.comp.config.kvs:
      value = expecting_spout_config[conf.key]
      if conf.type == topology_pb2.ConfigValueType.Value("STRING_VALUE"):
        self.assertEqual(value, conf.value)
      elif conf.type == topology_pb2.ConfigValueType.Value("PYTHON_SERIALIZED_VALUE"):
        self.assertEqual(value, conf.serialized_value)
      else:
        self.fail()

    # output stream
    self.assertEqual(len(spout.outputs), 2)
    for out_stream in spout.outputs:
      if out_stream.stream.id == "default":
        self.assertEqual(out_stream.stream.component_name, "spout")
        self.assertEqual(len(out_stream.schema.keys), 2)
      else:
        self.assertEqual(out_stream.stream.id, "error_stream")
        self.assertEqual(out_stream.stream.component_name, "spout")
        self.assertEqual(len(out_stream.schema.keys), 1)


    ### bolt protobuf ###
    self.assertEqual(len(proto_topo.bolts), 1)
    bolt = proto_topo.bolts[0]
    self.assertEqual(bolt.comp.name, "bolt")
    self.assertEqual(bolt.comp.spec, topology_pb2.ComponentObjectSpec.Value("PYTHON_CLASS_NAME"))
    self.assertEqual(bolt.comp.class_name, "bl_class")
    expecting_bolt_config = {"topology.component.parallelism": "4"}
    self.assertEqual(len(bolt.comp.config.kvs), len(expecting_bolt_config))
    conf = bolt.comp.config.kvs[0]
    self.assertEqual(conf.type, topology_pb2.ConfigValueType.Value("STRING_VALUE"))
    self.assertEqual(conf.value, expecting_bolt_config[conf.key])

    # out stream
    self.assertEqual(len(bolt.outputs), 0)

    # in stream
    self.assertEqual(len(bolt.inputs), 2)
    for in_stream in bolt.inputs:
      if in_stream.stream.id == "default":
        self.assertEqual(in_stream.stream.component_name, "spout")
        self.assertEqual(in_stream.gtype, topology_pb2.Grouping.Value("SHUFFLE"))
      else:
        self.assertEqual(in_stream.stream.id, "error_stream")
        self.assertEqual(in_stream.stream.component_name, "spout")
        self.assertEqual(in_stream.gtype, topology_pb2.Grouping.Value("ALL"))

    self.assertEqual(proto_topo.state, topology_pb2.TopologyState.Value("RUNNING"))

  def test_no_spout(self):
    with self.assertRaises(ValueError):
      # pylint:disable = unused-variable
      class JustBolt(Topology):
        bolt = HeronComponentSpec(None, "bl_class", False, 4)

  def test_class_dict_to_specs(self):
    # duplicate component name
    class_dict = {"spout": HeronComponentSpec("same_name", "sp_cls", True, 1),
                  "bolt": HeronComponentSpec("same_name", "bl_cls", False, 2)}
    with self.assertRaises(ValueError):
      TopologyType.class_dict_to_specs(class_dict)

  def test_add_spout_specs(self):
    # spout with no output
    spec = HeronComponentSpec("spout", "sp_cls", True, 1)
    with self.assertRaises(ValueError):
      TopologyType.add_spout_specs(spec, {})

  def test_add_bolt_specs(self):
    spec = HeronComponentSpec("bolt", "bl_cls", False, 1)
    with self.assertRaises(ValueError):
      TopologyType.add_bolt_specs(spec, {})

  def test_sanitize_config(self):
    # non-string key
    with self.assertRaises(TypeError):
      TopologyType._sanitize_config({['k', 'e', 'y']: "value"})
    with self.assertRaises(TypeError):
      TopologyType._sanitize_config({None: "value"})

    # convert boolean value
    ret = TopologyType._sanitize_config({"key": True})
    self.assertEqual(ret["key"], "true")
    ret = TopologyType._sanitize_config({"key": False})
    self.assertEqual(ret["key"], "false")

    # convert int and float
    ret = TopologyType._sanitize_config({"key": 10})
    self.assertEqual(ret["key"], "10")
    ret = TopologyType._sanitize_config({"key": -2400000})
    self.assertEqual(ret["key"], "-2400000")
    ret = TopologyType._sanitize_config({"key": 0.0000001})
    self.assertEqual(ret["key"], "1e-07")
    ret = TopologyType._sanitize_config({"key": -15.33333})
    self.assertEqual(ret["key"], "-15.33333")

    # non-string value -> should expect the same object
    ret = TopologyType._sanitize_config({"key": ['v', 'a', 'l', 'u', 'e']})
    self.assertEqual(ret["key"], ['v', 'a', 'l', 'u', 'e'])
    ret = TopologyType._sanitize_config({"key": None})
    self.assertEqual(ret["key"], None)

  def test_get_heron_options_from_env(self):
    test_value = "cmdline.key.1=/tmp/directory,cmdline.with.space=hello%%%%world"
    expecting = {"cmdline.key.1": "/tmp/directory", "cmdline.with.space": "hello world"}
    os.environ["HERON_OPTIONS"] = test_value
    ret = TopologyType.get_heron_options_from_env()
    self.assertEqual(ret, expecting)

    # error
    os.environ.pop("HERON_OPTIONS")
    with self.assertRaises(RuntimeError):
      TopologyType.get_heron_options_from_env()

class TopologyBuilderTest(unittest.TestCase):
  def test_constructor(self):
    builder = TopologyBuilder("WordCount")
    self.assertEqual(builder.topology_name, "WordCount")

    with self.assertRaises(AssertionError):
      TopologyBuilder("Topology")

    with self.assertRaises(AssertionError):
      TopologyBuilder(123)

    with self.assertRaises(AssertionError):
      TopologyBuilder(None)

  def test_add_spec(self):
    builder = TopologyBuilder("Test")

    with self.assertRaises(ValueError):
      builder.add_spec(HeronComponentSpec(None, "path", True, 1))

    with self.assertRaises(TypeError):
      builder.add_spec(None)

    self.assertEqual(len(builder._specs), 0)

    # add 10 specs
    specs = []
    for i in range(10):
      specs.append(HeronComponentSpec(str(i), "path", True, 1))
    builder.add_spec(*specs)
    self.assertEqual(len(builder._specs), 10)
