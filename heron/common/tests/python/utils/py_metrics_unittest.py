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

# pylint: disable=missing-docstring
from collections import namedtuple
from mock import Mock, patch
import unittest
import threading

from heron.common.src.python.utils.metrics.py_metrics import PyMetrics
import heron.common.src.python.constants as constants
import heron.common.tests.python.utils.mock_generator as mock_generator

Mem = namedtuple('Mem', ['rss', 'vms'])
Cputime = namedtuple('Cputime', ['system', 'user'])
Thread = namedtuple('Thread', ['id', 'user_time', 'system_time'])

class PyMetricsTest(unittest.TestCase):
  def setUp(self):
    metrics_collector = mock_generator.MockMetricsCollector()
    with patch("heron.common.src.python.config.system_config.get_sys_config",
               side_effect=lambda: {constants.HERON_METRICS_EXPORT_INTERVAL_SEC: 10}):
          self.py_metrics = PyMetrics(metrics_collector)
    self.py_metrics.process = Mock()
    self.py_metrics.process.cpu_times = Mock(return_value=Cputime(system=23, user=29))
    t1 = Thread(id=100, user_time=3, system_time=5)
    t2 = Thread(id=500, user_time=7, system_time=11)
    self.py_metrics.process.threads = Mock(return_value=[t1, t2])
    self.py_metrics.process.num_fds = Mock(return_value=3)
    self.py_metrics.process.memory_info = Mock(return_value=Mem(rss=13, vms=19))
  
  def test_gc(self):
    with patch("gc.get_count", side_effect=lambda:(1, 2, 3)):
      with patch("gc.get_threshold", side_effect=lambda:(4, 5, 6)):
        self.py_metrics.update_gc_stat()
    self.assertEqual(self.py_metrics.g1_count.get_value_and_reset(), 1)
    self.assertEqual(self.py_metrics.g2_count.get_value_and_reset(), 2)
    self.assertEqual(self.py_metrics.g3_count.get_value_and_reset(), 3)
    self.assertEqual(self.py_metrics.g1_threshold.get_value_and_reset(), 4)
    self.assertEqual(self.py_metrics.g2_threshold.get_value_and_reset(), 5)
    self.assertEqual(self.py_metrics.g3_threshold.get_value_and_reset(), 6)
    
  def test_cpu_times(self):
    self.py_metrics.update_cpu_time()
    self.assertEqual(self.py_metrics.sys_cpu_time.get_value_and_reset(), 23)
    self.assertEqual(self.py_metrics.user_cpu_time.get_value_and_reset(), 29)
    
  def test_threads_time(self):
    self.py_metrics.update_threads_time()
    tmap = self.py_metrics.threads.get_value_and_reset()
    self.assertEqual(tmap[100], (3, 5))
    self.assertEqual(tmap[500], (7, 11))
    
  def test_fd_num(self):
    self.py_metrics.update_fds()
    self.assertEqual(self.py_metrics.fd_nums.get_value_and_reset(), 3)
    
  def test_mem(self):
    self.py_metrics.update_memory_usage()
    self.assertEqual(self.py_metrics.physical_memory.get_value_and_reset(), 13)
    self.assertEqual(self.py_metrics.virtual_memory.get_value_and_reset(), 19)
    
