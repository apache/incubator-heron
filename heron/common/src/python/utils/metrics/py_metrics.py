# copyright 2016 twitter. all rights reserved.
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
""" Python program related metrics."""
import gc
import psutil
import traceback
from .metrics import AssignableMetrics
from .metrics_helper import BaseMetricsHelper
import heron.common.src.python.constants as constants
from heron.common.src.python.config import system_config
from heron.common.src.python.utils.log import Log

# pylint: disable=too-many-instance-attributes
class PyMetrics(BaseMetricsHelper):
  """Helper class to collect PyHeron program metrics"""
  def __init__(self, metrics_collector):
    self.process = psutil.Process()
    # total sys cpu time
    self.sys_cpu_time = AssignableMetrics(0)
    # total user cpu time
    self.user_cpu_time = AssignableMetrics(0)
    # threads cpu usage
    self.threads = AssignableMetrics(0)
    # number of open file descriptors
    self.fd_nums = AssignableMetrics(0)
    # number of threads
    self.num_threads = AssignableMetrics([])
    # rss: aka "Resident Set Size"
    # this is the non-swapped physical memory a process has used.
    self.physical_memory = AssignableMetrics(0)
    # vms: "Virtual Memory Size", this is the total
    # amount of virtual memory used by the process.
    self.virtual_memory = AssignableMetrics(0)
    # stats about three generations of GC
    # count is the number of objects in one generation
    # threshold is the collect frequency of one generation
    self.g1_count, self.g1_threshold = AssignableMetrics(0), AssignableMetrics(0)
    self.g2_count, self.g2_threshold = AssignableMetrics(0), AssignableMetrics(0)
    self.g3_count, self.g3_threshold = AssignableMetrics(0), AssignableMetrics(0)
    PY_SYS_CPU_TIME = '__py-sys-cpu-time-secs'
    PY_USER_CPU_TIME = '__py-user-cpu-time-secs'
    PY_FD_NUMS = '__py-file-descriptors-number'
    PY_PHYSICAL_MEMORY = '__py-physical-memory-byte'
    PY_VIRTUAL_MEMORY = '__py-virtual-memory-byte'
    PY_GC_GENERATION_1_COUNT = '__py-generation-1-count'
    PY_GC_GENERATION_2_COUNT = '__py-generation-2-count'
    PY_GC_GENERATION_3_COUNT = '__py-generation-3-count'
    PY_GC_GENERATION_1_THRESHOLD = '__py-generation-1-collection-threshold'
    PY_GC_GENERATION_2_THRESHOLD = '__py-generation-2-collection-threshold'
    PY_GC_GENERATION_3_THRESHOLD = '__py-generation-3-collection-threshold'
    self.metrics = {PY_SYS_CPU_TIME: self.sys_cpu_time,
                    PY_USER_CPU_TIME: self.user_cpu_time,
                    PY_FD_NUMS: self.fd_nums,
                    PY_PHYSICAL_MEMORY: self.physical_memory,
                    PY_VIRTUAL_MEMORY: self.virtual_memory,
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

  def update_cpu_time(self):
    try:
      t = self.process.cpu_times()
      self.sys_cpu_time.update(t.system)
      self.user_cpu_time.update(t.user)
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_threads_time(self):
    try:
      tt = self.process.threads()
      self.threads.update([(t.id, t.user_time, t.system_time) for t in tt])
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_fds(self):
    try:
      self.fd_nums.update(self.process.num_fds())
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_memory_usage(self):
    try:
      m = self.process.memory_info()
      self.physical_memory.update(m.rss)
      self.virtual_memory.update(m.vms)
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_gc_stat(self):
    try:
      c1, c2, c3 = gc.get_count()
      t1, t2, t3 = gc.get_threshold()
      self.g1_count.update(c1)
      self.g2_count.update(c2)
      self.g3_count.update(c3)
      self.t1_count.update(t1)
      self.t2_count.update(t2)
      self.t3_count.update(t3)
    except Exception as e:
      Log.error(traceback.format_exc(e))

  def update_all(self):
    self.update_cpu_time()
    self.update_threads_time()
    self.update_fds()
    self.update_memory_usage()
    self.update_gc_stat()
