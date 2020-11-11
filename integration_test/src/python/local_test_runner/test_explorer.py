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


"""test_kill_stmgr.py"""
import logging

from subprocess import run

from . import test_template


def run_explorer(*args):
  cmd = ["heron-explorer", *args]
  logging.debug(f"running command {cmd!r}")
  run(cmd, check=True, timeout=5)
  logging.debug(f"finished command {cmd!r}")


class TestExplorer(test_template.TestTemplate):

  def execute_test_case(self):
    from getpass import getuser
    from time import sleep
    cre = f"{self.params['cluster']}/{getuser()}/default"
    topology = self.params["topologyName"]
    sleep(2)
    tracker_option = f"--tracker-url=http://127.0.0.1:{self.params['trackerPort']}"
    run_explorer("clusters", tracker_option)

    cre_parts = cre.split("/")
    for i in range(len(cre_parts)):
      run_explorer("topologies", tracker_option, "/".join(cre_parts[:i+1]))

    run_explorer("logical-plan", tracker_option, cre, topology)
    run_explorer("logical-plan", tracker_option, cre, topology, "--component-type=bolts")
    run_explorer("logical-plan", tracker_option, cre, topology, "--component-type=spouts")

    run_explorer("physical-plan", "containers", tracker_option, cre, topology)
    run_explorer("physical-plan", "containers", tracker_option, cre, topology, "--id=1")
    run_explorer("physical-plan", "metrics", tracker_option, cre, topology)
