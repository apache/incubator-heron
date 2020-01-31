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

""" Python program related metrics."""
import gc
import resource
import traceback

from heronpy.api.metrics import AssignableMetrics

import heron.instance.src.python.utils.system_constants as constants
from heron.instance.src.python.utils import system_config
from heron.common.src.python.utils.log import Log

from .metrics_helper import BaseMetricsHelper

# pylint: disable=too-many-instance-attributes
class PyMetrics(BaseMetricsHelper):
  """Helper class to collect PyHeron program metrics"""
  def __init__(self, metrics_collector):
    # total sys CPU time
    self.sys_cpu_time = AssignableMetrics(0)
    # total user CPU time
    self.user_cpu_time = AssignableMetrics(0)
    # threads CPU usage. Not supported
    # Add it back when we find an alternative to psutil
    # self.threads = MultiAssignableMetrics()
    # number of open file descriptors. Not supported
    # Add it back when we find an alternative to psutil
    # self.fd_nums = AssignableMetrics(0)
    # rss: aka "Resident Set Size"
    # this is the non-swapped physical memory a process has used.
    self.physical_memory = AssignableMetrics(0)
    # vms: "Virtual Memory Size", this is the total
    # amount of virtual memory used by the process. Not supported
    # Add it back when we find an alternative to psutil
    # self.virtual_memory = AssignableMetrics(0)
    # stats about three generations of GC
    # count is the number of objects in one generation
    # threshold is the collect frequency of one generation
    self.g1_count, self.g1_threshold = AssignableMetrics(0), AssignableMetrics(0)
    self.g2_count, self.g2_threshold = AssignableMetrics(0), AssignableMetrics(0)
    self.g3_count, self.g3_threshold = AssignableMetrics(0), AssignableMetrics(0)
    PY_SYS_CPU_TIME = '__py-sys-cpu-time-secs'
    PY_USER_CPU_TIME = '__py-user-cpu-time-secs'
    # PY_FD_NUMS = '__py-file-descriptors-number'
    PY_PHYSICAL_MEMORY = '__py-physical-memory-byte'
    # PY_VIRTUAL_MEMORY = '__py-virtual-memory-byte'
    PY_GC_GENERATION_1_COUNT = '__py-generation-1-count'
    PY_GC_GENERATION_2_COUNT = '__py-generation-2-count'
    PY_GC_GENERATION_3_COUNT = '__py-generation-3-count'
    PY_GC_GENERATION_1_THRESHOLD = '__py-generation-1-collection-threshold'
    PY_GC_GENERATION_2_THRESHOLD = '__py-generation-2-collection-threshold'
    PY_GC_GENERATION_3_THRESHOLD = '__py-generation-3-collection-threshold'
    self.metrics = {PY_SYS_CPU_TIME: self.sys_cpu_time,
                    PY_USER_CPU_TIME: self.user_cpu_time,
                    # PY_FD_NUMS: self.fd_nums,
                    PY_PHYSICAL_MEMORY: self.physical_memory,
                    # PY_VIRTUAL_MEMORY: self.virtual_memory,
                    PY_GC_GENERATION_1_COUNT: self.g1_count,
                    PY_GC_GENERATION_2_COUNT: self.g2_count,
                    PY_GC_GENERATION_3_COUNT: self.g3_count,
                    PY_GC_GENERATION_1_THRESHOLD: self.g1_threshold,
                    PY_GC_GENERATION_2_THRESHOLD: self.g2_threshold,
                    PY_GC_GENERATION_3_THRESHOLD: self.g3_threshold}
    super(PyMetrics, self).__init__(self.metrics)
    sys_config = system_config.get_sys_config()
    interval = float(sys_config[constants.HERON_METRICS_EXPORT_INTERVAL_SEC])
    self.register_metrics(metrics_collector, interval)

  def update_cpu_and_memory_metrics(self):
    try:
      r = resource.getrusage(resource.RUSAGE_SELF)
      self.sys_cpu_time.update(r.ru_stime)
      self.user_cpu_time.update(r.ru_utime)
      self.physical_memory.update(r.ru_maxrss)
      # self.virtual_memory.update(m.vms)
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_threads_time(self):
    # try:
    #   for t in self.process.threads():
    #     self.threads.update(t.id, (t.user_time, t.system_time))
    # except Exception as e:
    #   Log.error(traceback.format_exc(e))
    pass

  def update_fds(self):
    # try:
    #   self.fd_nums.update(self.process.num_fds())
    # except Exception as e:
    #   Log.error(traceback.format_exc(e))
    pass

  def update_gc_stat(self):
    try:
      c1, c2, c3 = gc.get_count()
      t1, t2, t3 = gc.get_threshold()
      self.g1_count.update(c1)
      self.g2_count.update(c2)
      self.g3_count.update(c3)
      self.g1_threshold.update(t1)
      self.g2_threshold.update(t2)
      self.g3_threshold.update(t3)
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_all(self):
    self.update_cpu_and_memory_metrics()
    self.update_threads_time()
    self.update_fds()
    self.update_gc_stat()
