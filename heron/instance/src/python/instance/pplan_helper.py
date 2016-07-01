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
import socket

from heron.proto import topology_pb2

class PhysicalPlanHelper(object):
  def __init__(self, pplan, instance_id):
    self.pplan = pplan
    self.my_instance_id = instance_id
    self.my_instance = None

    # get my instance
    for instance in pplan.instances:
      if instance.instance_id == self.my_instance_id:
        self.my_instance = instance
        break

    if self.my_instance is None:
      raise RuntimeError("There was no instance that matched my id " + self.my_instance_id)

    self.my_component_name = self.my_instance.info.component_name
    self.my_task_id = self.my_instance.info.task_id

    # get spout or bolt
    self.my_spbl, self.is_spout = self._get_my_spout_or_bolt(pplan.topology)

    # output schema
    self.output_schema = dict()
    outputs = self.my_spbl.outputs

    for out_stream in outputs:
      self.output_schema[out_stream.stream.id] = len(out_stream.schema.keys)

    self.hostname = socket.gethostname()
    self.my_component = self.my_spbl.comp

    # TODO: topology.proto -- Component needs to change: Add python_class_name
    # TODO: implement CustomGrouping stuff

  def _get_my_spout_or_bolt(self, topology):
    my_spbl = None
    for spbl in (topology.spouts + topology.bolts):
      if spbl.comp.name == self.my_component_name:
        if my_spbl is not None:
          raise RuntimeError("Duplicate my component found")
        my_spbl = spbl

    if isinstance(spbl, topology_pb2.Spout):
      is_spout = True
    elif isinstance(spbl, topology_pb2.Bolt):
      is_spout = False
    else:
      raise RuntimeError("My component neither spout nor bolt")

    return spbl, is_spout

  def get_topology_state(self):
    return self.pplan.topology.state
