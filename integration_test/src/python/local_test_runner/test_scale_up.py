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


"""test_scale_up.py"""
import logging
import subprocess

from ..common import status
from . import test_template

class TestScaleUp(test_template.TestTemplate):

  expected_container_count = 1
  expected_instance_count = 3

  def get_expected_container_count(self):
    return self.expected_container_count

  def get_expected_min_instance_count(self):
    return self.expected_instance_count

  def execute_test_case(self):
    scale_up(self.params['cliPath'], self.params['cluster'], self.params['topologyName'])
    self.expected_container_count += 1
    self.expected_instance_count += 1

  def pre_check_results(self, physical_plan_json):

    instances = physical_plan_json['instances']
    instance_count = len(instances)
    if instance_count != self.expected_instance_count:
      raise status.TestFailure(f"Found {instance_count} instances but expected {self.expected_instance_count}: {instances}")

def scale_up(heron_cli_path, test_cluster, topology_name):
  splitcmd = [
      heron_cli_path, 'update', '--verbose', test_cluster, topology_name,
      '--component-parallelism=identity-bolt:2'
  ]
  logging.info("Increasing number of component instances: %s", splitcmd)
  if subprocess.call(splitcmd) != 0:
    raise status.TestFailure(f"Unable to update topology {topology_name}")
  logging.info("Increased number of component instances")
