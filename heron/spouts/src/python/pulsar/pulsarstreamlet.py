# Copyright 2016 - Twitter, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Streamlet for Apache Pulsar"""

from heron.dsl.src.python import Streamlet, OperationType
from .pulsarspout import PulsarSpout

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class PulsarStreamlet(Streamlet):
  """Streamlet facade on top of PulsarSpout"""
  def __init__(self, service_url, topic_name, stage_name=None, parallelism=None,
               receive_timeout_ms=None, input_schema=None):
    self._pulsar_service_url = service_url
    self._pulsar_topic_name = topic_name
    self._pulsar_receive_timeout_ms = receive_timeout_ms
    self._pulsar_input_schema = input_schema
    super(PulsarStreamlet, self).__init__(operation=OperationType.Input,
                                          stage_name=stage_name,
                                          parallelism=parallelism)

  @staticmethod
  def pulsarStreamlet(service_url, topic_name, stage_name=None, parallelism=None,
                      receive_timeout_ms=None, input_schema=None):
    if service_url is None:
      raise RuntimeError("Pulsar Service Url cannot be None")
    if topic_name is None:
      raise RuntimeError("Pulsar Topic Name cannot be None")
    return PulsarStreamlet(service_url, topic_name, stage_name=stage_name,
                           parallelism=parallelism, receive_timeout_ms=receive_timeout_ms,
                           input_schema=input_schema)

  def _build(self, bldr, stage_names):
    if self._parallelism is None:
      self._parallelism = 1
    if self._stage_name is None:
      index = 1
      self._stage_name = "pulsarspout-" + self._pulsar_topic_name
      while self._stage_name in stage_names:
        index = index + 1
        self._stage_name = "pulsarspout-" + self._pulsar_topic_name + str(index)
      config = {PulsarSpout.serviceUrl : self._pulsar_service_url,
                PulsarSpout.topicName : self._pulsar_topic_name}
      if self._pulsar_receive_timeout_ms is not None:
        config.update({PulsarSpout.receiveTimeoutMs : self._pulsar_receive_timeout_ms})
      if self._pulsar_input_schema is not None:
        config.update({PulsarSpout.deserializer : self._pulsar_input_schema})
      bldr.add_spout(self._stage_name, PulsarSpout, par=self._parallelism, config=config)
    return bldr
