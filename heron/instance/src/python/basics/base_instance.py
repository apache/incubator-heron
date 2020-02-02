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

'''base_instance.py: module for base component (base for spout/bolt) and its spec'''

import logging
import traceback

from abc import abstractmethod

from heron.common.src.python.utils.log import Log
from heron.proto import tuple_pb2
from heron.instance.src.python.utils.misc import SerializerHelper
from heron.instance.src.python.utils.misc import OutgoingTupleHelper
from heron.instance.src.python.utils import system_config
import heron.instance.src.python.utils.system_constants as system_constants
import heron.common.src.python.pex_loader as pex_loader

import heronpy.api.global_metrics as global_metrics
import heronpy.api.api_constants as api_constants
from heronpy.api.state.stateful_component import StatefulComponent

# pylint: disable=too-many-instance-attributes
class BaseInstance(object):
  """The base class for heron bolt/spout instance

  Implements the following functionality:
  1. Basic output collector API and pushing tuples to Out-Stream
  2. Run tasks continually

  :ivar pplan_helper: Physical Plan Helper for this component
  :ivar in_stream:    In-Stream Heron Communicator
  :ivar output_helper: Outgoing Tuple Helper
  :ivar serializer: Implementation of Heron Serializer
  """
  make_data_tuple = lambda _: tuple_pb2.HeronDataTuple()

  def __init__(self, pplan_helper, in_stream, out_stream, looper):
    self.pplan_helper = pplan_helper
    self.in_stream = in_stream
    self.output_helper = OutgoingTupleHelper(self.pplan_helper, out_stream)
    self.looper = looper
    self.sys_config = system_config.get_sys_config()

    # will set a root logger here
    self.logger = logging.getLogger()
    context = pplan_helper.context
    mode = context.get_cluster_config().get(api_constants.TOPOLOGY_RELIABILITY_MODE,
                                            api_constants.TopologyReliabilityMode.ATMOST_ONCE)
    self.is_stateful = bool(mode == api_constants.TopologyReliabilityMode.EFFECTIVELY_ONCE)
    self._stateful_state = None
    self.serializer = SerializerHelper.get_serializer(pplan_helper.context)
    self._initialized_global_metrics = False

  def log(self, message, level=None):
    """Log message, optionally providing a logging level

    It is compatible with StreamParse API.

    :type message: str
    :param message: the log message to send
    :type level: str
    :param level: the logging level,
                  one of: trace (=debug), debug, info, warn or error (default: info)
    """
    if level is None:
      _log_level = logging.INFO
    else:
      if level == "trace" or level == "debug":
        _log_level = logging.DEBUG
      elif level == "info":
        _log_level = logging.INFO
      elif level == "warn":
        _log_level = logging.WARNING
      elif level == "error":
        _log_level = logging.ERROR
      else:
        raise ValueError("%s is not supported as logging level" % str(level))

    self.logger.log(_log_level, message)

  def admit_data_tuple(self, stream_id, data_tuple, tuple_size_in_bytes):
    self.output_helper.add_data_tuple(stream_id, data_tuple, tuple_size_in_bytes)

  def admit_control_tuple(self, control_tuple, tuple_size_in_bytes, is_ack):
    self.output_helper.add_control_tuple(control_tuple, tuple_size_in_bytes, is_ack)

  def admit_ckpt_state(self, ckpt_id, ckpt_state):
    self.output_helper.add_ckpt_state(ckpt_id, self.serializer.serialize(ckpt_state))

  def get_total_data_emitted_in_bytes(self):
    return self.output_helper.total_data_emitted_in_bytes

  def load_py_instance(self, is_spout):
    """Loads user defined component (spout/bolt)"""
    try:
      if is_spout:
        spout_proto = self.pplan_helper.get_my_spout()
        py_classpath = spout_proto.comp.class_name
        self.logger.info("Loading Spout from: %s", py_classpath)
      else:
        bolt_proto = self.pplan_helper.get_my_bolt()
        py_classpath = bolt_proto.comp.class_name
        self.logger.info("Loading Bolt from: %s", py_classpath)

      pex_loader.load_pex(self.pplan_helper.topology_pex_abs_path)
      spbl_class = pex_loader.import_and_get_class(self.pplan_helper.topology_pex_abs_path,
                                                   py_classpath)
    except Exception as e:
      spbl = "spout" if is_spout else "bolt"
      self.logger.error(traceback.format_exc())
      raise RuntimeError("Error when loading a %s from pex: %s" % (spbl, str(e)))
    return spbl_class

  def handle_initiate_stateful_checkpoint(self, ckptmsg, component):
    Log.info("Received initiate state checkpoint message for %s" % ckptmsg.checkpoint_id)
    if not self.is_stateful:
      raise RuntimeError("Received state checkpoint message but we are not stateful topology")
    if isinstance(component, StatefulComponent):
      component.pre_save(ckptmsg.checkpoint_id)
    else:
      Log.info("Trying to checkponit a non stateful component. Send empty state")
    self.admit_ckpt_state(ckptmsg.checkpoint_id, self._stateful_state)

  def clear_collector(self):
    self.output_helper.clear()

  def start(self, stateful_state):
    self._stateful_state = stateful_state
    self.start_component(stateful_state)
    context = self.pplan_helper.context
    context.invoke_hook_prepare()

    # prepare global metrics
    if not self._initialized_global_metrics:
      interval = float(self.sys_config[system_constants.HERON_METRICS_EXPORT_INTERVAL_SEC])
      collector = context.get_metrics_collector()
      global_metrics.init(collector, interval)
      self._initialized_global_metrics = True

    # prepare for custom grouping
    self.pplan_helper.prepare_custom_grouping(context)

  def stop(self):
    self.pplan_helper.context.invoke_hook_cleanup()
    self.stop_component()

  ##################################################################
  # The followings are to be implemented by Spout/Bolt independently
  ##################################################################

  @abstractmethod
  def start_component(self, stateful_state):
    """Do the basic setup for Heron Instance"""
    raise NotImplementedError()

  @abstractmethod
  def stop_component(self):
    """Do the basic clean for Heron Instance

    Note that this method is not guaranteed to be invoked
    """
    raise NotImplementedError()

  @abstractmethod
  def process_incoming_tuples(self):
    """Should be called when a tuple was buffered into in_stream"""
    raise NotImplementedError()

  @abstractmethod
  def invoke_activate(self):
    """Activate the instance"""
    raise NotImplementedError()

  @abstractmethod
  def invoke_deactivate(self):
    """Deactivate the instance"""
    raise NotImplementedError()
