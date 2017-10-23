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
'''windowconfig.py: module for defining windowconfig'''

import datetime

class WindowConfig(object):
  """WindowConfig allows streamlet API users to program window configuration for operations
     that rely on windowing. Currently we only support time/count based
     sliding/tumbling windows.
  """
  def __init__(self, window_duration, slide_interval):
    self._window_duration = window_duration
    self._slide_interval = slide_interval
    if self._window_duration.microseconds > 0 or self._slide_interval.microseconds > 0:
      raise RuntimeError("Python Windowing curently only supports second resolution")

  @staticmethod
  def create_tumbling_window(window_duration):
    return WindowConfig(window_duration, window_duration)

  @staticmethod
  def create_sliding_window(window_duration, slide_interval):
    if isinstance(window_duration, int):
      window_duration = datetime.timedelta(seconds=window_duration)
    if isinstance(slide_interval, int):
      slide_interval = datetime.timedelta(seconds=slide_interval)
    if not isinstance(window_duration, datetime.timedelta):
      raise RuntimeError("Window Duration has to be of type datetime.timedelta")
    if not isinstance(slide_interval, datetime.timedelta):
      raise RuntimeError("Slide Interval has to be of type datetime.timedelta")
    return WindowConfig(window_duration, slide_interval)
