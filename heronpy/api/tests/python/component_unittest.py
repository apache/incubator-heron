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


# pylint: disable=missing-docstring
# pylint: disable=protected-access
import unittest

from heronpy.api.component.component_spec import HeronComponentSpec, GlobalStreamId
from heronpy.api.stream import Grouping, Stream

class ComponentSpecTest(unittest.TestCase):
  def test_sanitize_args(self):
    # good args
    spec = HeronComponentSpec(
        name="string", python_class_path="string.path",
        is_spout=True,
        par=1
    )
    self.assertIsNotNone(spec)
    name_none_spec = HeronComponentSpec(name=None, python_class_path="string.path",
                                        is_spout=True, par=1)
    self.assertIsNotNone(name_none_spec)

    # bad name
    with self.assertRaises(AssertionError):
      HeronComponentSpec(123, "classpath", True, 1)
    with self.assertRaises(AssertionError):
      HeronComponentSpec(False, "classpath", True, 1)

    # bad classpath
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", {}, True, 1)
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", None, True, 1)

    # bad is_spout
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", 1, 1)
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", None, 1)

    # bad par
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", True, "1")
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", True, 1.35)
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", True, -21)
    with self.assertRaises(AssertionError):
      HeronComponentSpec("name", "classpath", True, None)

  def test_sanitize_config(self):
    # empty dict
    ret = HeronComponentSpec._sanitize_config({})
    self.assertEqual(ret, {})

    # non-dict given
    with self.assertRaises(TypeError):
      HeronComponentSpec._sanitize_config("{key: value}")
    with self.assertRaises(TypeError):
      HeronComponentSpec._sanitize_config(True)
    with self.assertRaises(TypeError):
      HeronComponentSpec._sanitize_config(None)

    # non-string key
    with self.assertRaises(TypeError):
      HeronComponentSpec._sanitize_config({['k', 'e', 'y']: "value"})
    with self.assertRaises(TypeError):
      HeronComponentSpec._sanitize_config({None: "value"})

    # convert boolean value
    ret = HeronComponentSpec._sanitize_config({"key": True})
    self.assertEqual(ret["key"], "true")
    ret = HeronComponentSpec._sanitize_config({"key": False})
    self.assertEqual(ret["key"], "false")

    # convert int and float
    ret = HeronComponentSpec._sanitize_config({"key": 10})
    self.assertEqual(ret["key"], "10")
    ret = HeronComponentSpec._sanitize_config({"key": -2400000})
    self.assertEqual(ret["key"], "-2400000")
    ret = HeronComponentSpec._sanitize_config({"key": 0.0000001})
    self.assertEqual(ret["key"], "1e-07")
    ret = HeronComponentSpec._sanitize_config({"key": -15.33333})
    self.assertEqual(ret["key"], "-15.33333")

    # non-string value -> should expect the same object
    ret = HeronComponentSpec._sanitize_config({"key": ['v', 'a', 'l', 'u', 'e']})
    self.assertEqual(ret["key"], ['v', 'a', 'l', 'u', 'e'])
    ret = HeronComponentSpec._sanitize_config({"key": None})
    self.assertEqual(ret["key"], None)

  def test_sanitize_inputs(self):
    # Note that _sanitize_inputs() should only be called after HeronComponentSpec's
    # name attribute is set

    # invalid inputs given as argument (valid ones are either dict, list, tuple or None)
    invalid_spec = HeronComponentSpec("name", "classpath", True, 1, inputs="string")
    with self.assertRaises(TypeError):
      invalid_spec._sanitize_inputs()

    invalid_spec = HeronComponentSpec("name", "classpath", True, 1, inputs=100)
    with self.assertRaises(TypeError):
      invalid_spec._sanitize_inputs()

    # dict <HeronComponentSpec -> Grouping>
    from_spec = HeronComponentSpec("spout", "sp_clspath", True, 1)
    to_spec = HeronComponentSpec("bolt", "bl_clspath", False, 1,
                                 inputs={from_spec: Grouping.SHUFFLE})
    ret = to_spec._sanitize_inputs()
    self.assertEqual(ret, {GlobalStreamId("spout", "default"): Grouping.SHUFFLE})

    from_spec = HeronComponentSpec("spout", "sp_clspath", True, 1)
    from_spec.outputs = [Stream(name='another_stream')]
    to_spec = HeronComponentSpec("bolt", "bl_clspath", False, 1,
                                 inputs={from_spec['another_stream']: Grouping.ALL})
    ret = to_spec._sanitize_inputs()
    self.assertEqual(ret, {GlobalStreamId("spout", "another_stream"): Grouping.ALL})

    # HeronComponentSpec's name attribute not set
    from_spec = HeronComponentSpec(None, "sp_clspath", True, 1)
    to_spec = HeronComponentSpec("bolt", "bl_clspath", False, 1,
                                 inputs={from_spec: Grouping.ALL})
    with self.assertRaises(RuntimeError):
      to_spec._sanitize_inputs()

    # dict <GlobalStreamId -> Grouping>
    inputs_dict = {GlobalStreamId("some_spout", "some_stream"): Grouping.NONE,
                   GlobalStreamId("another_spout", "default"): Grouping.fields(['word', 'count'])}
    spec = HeronComponentSpec("bolt", "classpath", False, 1, inputs=inputs_dict)
    ret = spec._sanitize_inputs()
    self.assertEqual(ret, inputs_dict)

    # list of HeronComponentSpec
    from_spec1 = HeronComponentSpec("spout1", "sp1_cls", True, 1)
    from_spec2 = HeronComponentSpec("spout2", "sp2_cls", True, 1)
    to_spec = HeronComponentSpec("bolt", "bl_cls", False, 1, inputs=[from_spec1, from_spec2])
    ret = to_spec._sanitize_inputs()
    self.assertEqual(ret, {GlobalStreamId("spout1", "default"): Grouping.SHUFFLE,
                           GlobalStreamId("spout2", "default"): Grouping.SHUFFLE})

    # HeronComponentSpec's name attribute not set
    from_spec = HeronComponentSpec(None, "sp_clspath", True, 1)
    to_spec = HeronComponentSpec("bolt", "bl_clspath", False, 1, inputs=[from_spec])
    with self.assertRaises(RuntimeError):
      to_spec._sanitize_inputs()

    # list of GlobalStreamId
    inputs_list = [GlobalStreamId("spout1", "default"), GlobalStreamId("spout2", "some_stream")]
    spec = HeronComponentSpec("bolt", "bl_cls", False, 1, inputs=inputs_list)
    ret = spec._sanitize_inputs()
    self.assertEqual(ret, dict(list(zip(inputs_list, [Grouping.SHUFFLE] * 2))))

    # list of neither GlobalStreamId nor HeronComponentSpec
    inputs_list = [None, 123, "string", [GlobalStreamId("sp", "default")]]
    spec = HeronComponentSpec("bolt", "bl_cls", False, 1, inputs=inputs_list)
    with self.assertRaises(ValueError):
      spec._sanitize_inputs()

  # pylint: disable=redefined-variable-type
  # pylint: disable=pointless-statement
  def test_sanitize_outputs(self):
    # outputs is None (no argument to outputs)
    spec = HeronComponentSpec("spout", "class", True, 1)
    ret = spec._sanitize_outputs()
    self.assertIsNone(ret)

    # outputs neither list nor tuple
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = "string"
    with self.assertRaises(TypeError):
      spec._sanitize_outputs()

    # output list contains a non-string and non-Stream object
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = ["string", False, 123]
    with self.assertRaises(TypeError):
      spec._sanitize_outputs()

    # output list is all string
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = ["string", "hello", "heron"]
    ret = spec._sanitize_outputs()
    self.assertEqual(ret, {"default": ["string", "hello", "heron"]})

    # output list has mixed stream
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = ["string", "hello", Stream(fields=["abc", "def"], name="another_stream"),
                    Stream(fields=["another", "default"], name="default")]
    ret = spec._sanitize_outputs()
    self.assertEqual(ret, {"default": ["string", "hello", "another", "default"],
                           "another_stream": ["abc", "def"]})

  def test_get_out_streamids(self):
    # outputs is none
    spec = HeronComponentSpec("spout", "class", True, 1)
    ret = spec.get_out_streamids()
    self.assertEqual(ret, set())

    # outputs neither list nor tuple
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = "string"
    with self.assertRaises(TypeError):
      spec.get_out_streamids()

    # outputs sane
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = ["string", "hello", Stream(fields=["abc", "def"], name="another_stream"),
                    Stream(fields=["another", "default"], name="default")]
    ret = spec.get_out_streamids()
    self.assertEqual(ret, {"default", "another_stream"})

  def test_get_item(self):
    # HeronComponentSpec name set
    spec = HeronComponentSpec("spout", "class", True, 1)
    spec.outputs = ["string", "hello", Stream(fields=["abc", "def"], name="another_stream"),
                    Stream(fields=["another", "default"], name="default")]
    ret = spec['another_stream']
    self.assertEqual(ret, GlobalStreamId("spout", "another_stream"))

    # HeronComponentSpec name not set
    spec = HeronComponentSpec(None, "class", True, 1)
    spec.outputs = ["string", "hello", Stream(fields=["abc", "def"], name="another_stream"),
                    Stream(fields=["another", "default"], name="default")]
    ret = spec['default']
    self.assertEqual(ret, GlobalStreamId(spec, "default"))

    # stream id not registered
    spec = HeronComponentSpec(None, "class", True, 1)
    spec.outputs = ["string", "hello", Stream(fields=["abc", "def"], name="another_stream"),
                    Stream(fields=["another", "default"], name="default")]
    with self.assertRaises(ValueError):
      spec['non_existent_stream']

class GlobalStreamIdTest(unittest.TestCase):
  def test_constructor(self):
    # component id not string nor HeronComponentSpec
    with self.assertRaises(TypeError):
      GlobalStreamId(componentId=123, streamId="default")
    # stream id not string
    with self.assertRaises(TypeError):
      GlobalStreamId(componentId="component", streamId=12345)

  def test_component_id_property(self):
    # component id is string
    gsi = GlobalStreamId(componentId="component", streamId="stream")
    self.assertEqual(gsi.component_id, "component")

    # component id is HeronComponentSpec with name
    spec = HeronComponentSpec("spout", "class", True, 1)
    gsi = GlobalStreamId(spec, "stream")
    self.assertEqual(gsi.component_id, "spout")

    # component id is HeronComponentSpec without name
    spec = HeronComponentSpec(None, "class", True, 1)
    gsi = GlobalStreamId(spec, "stream")
    # expecting "<No name available for HeronComponentSpec yet, uuid: %s>"
    self.assertIn(spec.uuid, gsi.component_id)
