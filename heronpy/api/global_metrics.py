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
""" Singleton class which exposes a simple globally available counter for heron jobs.

It serves the same functionality as GlobalMetrics.java
"""
import threading
from heronpy.api.metrics import MultiCountMetric

metricsContainer = MultiCountMetric()
registered = False
root_name = '__auto__'

lock = threading.Lock()

def incr(key, to_add=1):
  metricsContainer.incr(key, to_add)

def safe_incr(key, to_add=1):
  with lock:
    metricsContainer.incr(key, to_add)

def init(metrics_collector, metrics_bucket):
  with lock:
    global registered
    if not registered:
      metrics_collector.register_metric(root_name, metricsContainer, metrics_bucket)
      registered = True
