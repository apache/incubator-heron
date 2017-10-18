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

from heronpy.streamlet.src.python.streamlet import Streamlet
from heronpy.connectors.pulsar.src.python.pulsarspout import PulsarSpout

# pylint: disable=access-member-before-definition
# pylint: disable=attribute-defined-outside-init
class PulsarStreamlet(Streamlet):
  """Streamlet facade on top of PulsarSpout"""
  def __init__(self, service_url, topic_name, stage_name=None, parallelism=None,
               receive_timeout_ms=None, input_schema=None):
    super(PulsarStreamlet, self).__init__(parents=[],
                                          stage_name=stage_name,
                                          parallelism=parallelism)
    self._pulsar_service_url = service_url
    self._pulsar_topic_name = topic_name
    self._pulsar_receive_timeout_ms = receive_timeout_ms
    self._pulsar_input_schema = input_schema

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

  def _calculate_stage_name(self, existing_stage_names):
    index = 1
    stage_name = "pulsarspout-" + self._pulsar_topic_name
    while stage_name in existing_stage_names:
      index = index + 1
      stage_name = "pulsarspout-" + self._pulsar_topic_name + str(index)
    return stage_name

  def _build_this(self, bldr):
    config = {PulsarSpout.serviceUrl : self._pulsar_service_url,
              PulsarSpout.topicName : self._pulsar_topic_name}
    if self._pulsar_receive_timeout_ms is not None:
      config.update({PulsarSpout.receiveTimeoutMs : self._pulsar_receive_timeout_ms})
    if self._pulsar_input_schema is not None:
      config.update({PulsarSpout.deserializer : self._pulsar_input_schema})
    bldr.add_spout(self._stage_name, PulsarSpout, par=self._parallelism, config=config)
