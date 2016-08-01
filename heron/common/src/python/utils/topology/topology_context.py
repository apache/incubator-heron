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
'''topology_context.py'''

from heron.common.src.python.utils.metrics import MetricsCollector

class TopologyContext(dict):
  """Helper Class for Topology Context, inheriting from dict"""
  # topology as supplied by the cluster overloaded by any component specific config
  CONFIG = 'config'
  # topology protobuf
  TOPOLOGY = 'topology'
  # dict <task_id -> component_id>
  TASK_TO_COMPONENT_MAP = 'task_to_component'
  # my task_id
  TASK_ID = 'task_id'
  # dict <component_id -> list of its inputs>
  INPUTS = 'inputs'
  # dict <component_id -> list of its outputs>
  OUTPUTS = 'outputs'
  # dict <component_id -> <stream_id -> fields>>
  COMPONENT_TO_OUT_FIELDS = 'comp_to_out_fields'

  METRICS_COLLECTOR = 'metrics_collector'

  def __init__(self, config, topology, task_to_component, my_task_id, metrics_collector, **kwargs):
    super(TopologyContext, self).__init__(**kwargs)
    self[self.CONFIG] = config
    self[self.TOPOLOGY] = topology
    self[self.TASK_TO_COMPONENT_MAP] = task_to_component
    self[self.TASK_ID] = my_task_id
    self[self.METRICS_COLLECTOR] = metrics_collector
    inputs, outputs, out_fields = self._get_inputs_and_outputs_and_outfields(topology)
    self[self.INPUTS] = inputs
    self[self.OUTPUTS] = outputs
    self[self.COMPONENT_TO_OUT_FIELDS] = out_fields

  def get_metrics_collector(self):
    """Returns this context's metrics collector"""
    if TopologyContext.METRICS_COLLECTOR not in self or \
        not isinstance(self.get(TopologyContext.METRICS_COLLECTOR), MetricsCollector):
      raise RuntimeError("Metrics collector is not registered in this context")
    return self.get(TopologyContext.METRICS_COLLECTOR)

  def register_metric(self, name, metric, time_bucket_in_sec):
    """Registers a new metric to this context"""
    collector = self.get_metrics_collector()
    collector.register_metric(name, metric, time_bucket_in_sec)

  @classmethod
  def _get_inputs_and_outputs_and_outfields(cls, topology):
    inputs = {}
    outputs = {}
    out_fields = {}
    for spout in topology.spouts:
      inputs[spout.comp.name] = []  # spout doesn't have any inputs
      outputs[spout.comp.name] = spout.outputs
      out_fields.update(cls._get_output_to_comp_fields(spout.outputs))

    for bolt in topology.bolts:
      inputs[bolt.comp.name] = bolt.inputs
      outputs[bolt.comp.name] = bolt.outputs
      out_fields.update(cls._get_output_to_comp_fields(bolt.outputs))

    return inputs, outputs, out_fields

  @staticmethod
  def _get_output_to_comp_fields(outputs):
    out_fields = {}

    for out_stream in outputs:
      comp_name = out_stream.stream.component_name
      stream_id = out_stream.stream.id

      if comp_name not in out_fields:
        out_fields[comp_name] = dict()

      # get the fields of a particular output stream
      ret = []
      for kt in out_stream.schema.keys:
        ret.append(kt.key)

      out_fields[comp_name][stream_id] = tuple(ret)
    return out_fields
