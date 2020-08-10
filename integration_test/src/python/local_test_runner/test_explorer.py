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

from contextlib import contextmanager
from subprocess import run, Popen
from time import sleep

from . import test_template


@contextmanager
def bound_subprocess(popen: Popen) -> None:
  with popen:
    logging.debug(f"starting {popen!r}")
    try:
      yield
    finally:
      logging.debug(f"killing {popen!r}")
      popen.kill()
      popen.communicate()

def run_explorer(*args):
  cmd = ["heron-explorer", *args]
  logging.debug(f"running command {cmd!r}")
  run(cmd, check=True, timeout=5)
  logging.debug(f"finished command {cmd!r}")


class TestExplorer(test_template.TestTemplate):


  def execute_test_case(self):
    from getpass import getuser
    cre = f"{self.params['cluster']}/{getuser()}/default"
    topology = self.params["topologyName"]
    # heron-explorer depens on heron-tracker, so start an instance as it is not started
    # by heron-cli when running against the local "cluster"
    with bound_subprocess(Popen(["heron-tracker"])):
      sleep(2)
      run_explorer("clusters")

      cre_parts = cre.split("/")
      for i in range(len(cre_parts)):
        run_explorer("topologies", "/".join(cre_parts[:i+1]))

      run_explorer("logical-plan", cre, topology)
      run_explorer("logical-plan", cre, topology, "--component-type=bolts")
      run_explorer("logical-plan", cre, topology, "--component-type=spouts")

      run_explorer("physical-plan", "containers", cre, topology)
      run_explorer("physical-plan", "containers", cre, topology, "--id=1")
      run_explorer("physical-plan", "metrics", cre, topology)
