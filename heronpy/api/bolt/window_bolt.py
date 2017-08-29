# Copyright 2016 - Twitter. All rights reserved
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
'''window_bolt.py: API for defining windowed bolts in Heron'''
from abc import abstractmethod
from collections import namedtuple, deque
import time
from heronpy.api.bolt.bolt import Bolt
import heronpy.api.api_constants as api_constants
from heronpy.api.state.stateful_component import StatefulComponent

WindowContext = namedtuple('WindowContext', ('start', 'end'))

class SlidingWindowBolt(Bolt, StatefulComponent):
  """SlidingWindowBolt is a higer level bolt for Heron users who want to deal with
     batches of tuples belonging to a certain time window. This bolt keeps track of
     managing the window, adding/expiring tuples based on window configuration.
     This way users will just have to deal with writing processWindow function
  """
  WINDOW_DURATION_SECS = 'slidingwindowbolt_duration_secs'
  WINDOW_SLIDEINTERVAL_SECS = 'slidingwindowbolt_slideinterval_secs'

  # pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self.saved_state = stateful_state

  # pylint: disable=unused-argument
  def pre_save(self, checkpoint_id):
    self.saved_state['tuples'] = self.current_tuples

  @abstractmethod
  def processWindow(self, window_info, tuples):
    """The main interface that needs to be implemented.

    This function is called every WINDOW_SLIDEINTERVAL_SECS seconds
    and contains the data in the last WINDOW_DURATION_SECS seconds
    in a list tuples

    :type window_info: :class:`WindowContext`
    :param window_info: The information about the window

    :type tuples: :class:`list of Tuples`
    :param tuples: The list of tuples in this window
    """
    pass

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    """We initialize the window duration and slide interval
    """
    if SlidingWindowBolt.WINDOW_DURATION_SECS in config:
      self.window_duration = int(config[SlidingWindowBolt.WINDOW_DURATION_SECS])
    else:
      self.logger.fatal("Window Duration has to be specified in the config")
    if SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS in config:
      self.slide_interval = int(config[SlidingWindowBolt.WINDOW_SLIDEINTERVAL_SECS])
    else:
      self.slide_interval = self.window_duration
    if self.slide_interval > self.window_duration:
      self.logger.fatal("Slide Interval should be <= Window Duration")

    # By modifying the config, we are able to setup the tick timer
    config[api_constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS] = str(self.slide_interval)
    self.current_tuples = deque()
    if hasattr(self, 'saved_state'):
      if 'tuples' in self.saved_state:
        self.current_tuples = self.saved_state['tuples']

  def process(self, tup):
    """Process a single tuple of input

    We add the (time, tuple) pair into our current_tuples. And then look for expiring
    elemnents
    """
    curtime = int(time.time())
    self.current_tuples.append((tup, curtime))
    self._expire(curtime)

  def _expire(self, tm):
    while len(self.current_tuples) > 0:
      if tm - self.window_duration > self.current_tuples[0][1]:
        (tup, _) = self.current_tuples.popleft()
        self.ack(tup)
      else:
        break

  # pylint: disable=unused-argument
  # pylint: disable=unused-variable
  def process_tick(self, tup):
    """Called every slide_interval
    """
    curtime = int(time.time())
    window_info = WindowContext(curtime - self.window_duration, curtime)
    tuple_batch = []
    for (tup, tm) in self.current_tuples:
      tuple_batch.append(tup)
    self.processWindow(window_info, tuple_batch)
    self._expire(curtime)


class TumblingWindowBolt(Bolt, StatefulComponent):
  """TumblingWindowBolt is a higer level bolt for Heron users who want to deal with
     batches of tuples belonging to a certain time window. This bolt keeps track of
     managing the window, adding/expiring tuples based on window configuration.
     This way users will just have to deal with writing processWindow function
  """
  WINDOW_DURATION_SECS = 'tumblingwindowbolt_duration_secs'

  # pylint: disable=attribute-defined-outside-init
  def init_state(self, stateful_state):
    self.saved_state = stateful_state

  # pylint: disable=unused-argument
  def pre_save(self, checkpoint_id):
    self.saved_state['tuples'] = self.current_tuples

  @abstractmethod
  def processWindow(self, window_info, tuples):
    """The main interface that needs to be implemented.

    This function is called every WINDOW_DURATION_SECS seconds
    and contains the data in the last WINDOW_DURATION_SECS seconds
    in a list tuples

    :type window_info: :class:`WindowContext`
    :param window_info: The information about the window

    :type tuples: :class:`list of Tuples`
    :param tuples: The list of tuples in this window
    """
    pass

  # pylint: disable=unused-argument
  def initialize(self, config, context):
    """We initialize the window duration and slide interval
    """
    if TumblingWindowBolt.WINDOW_DURATION_SECS in config:
      self.window_duration = int(config[TumblingWindowBolt.WINDOW_DURATION_SECS])
    else:
      self.logger.fatal("Window Duration has to be specified in the config")

    # By modifying the config, we are able to setup the tick timer
    config[api_constants.TOPOLOGY_TICK_TUPLE_FREQ_SECS] = str(self.window_duration)
    self.current_tuples = deque()
    if hasattr(self, 'saved_state'):
      if 'tuples' in self.saved_state:
        self.current_tuples = self.saved_state['tuples']

  def process(self, tup):
    """Process a single tuple of input

    We simply add the tuple into our current_tuples.
    """
    self.current_tuples.append(tup)

  # pylint: disable=unused-argument
  # pylint: disable=unused-variable
  def process_tick(self, tup):
    """Called every window_duration
    """
    curtime = int(time.time())
    window_info = WindowContext(curtime - self.window_duration, curtime)
    self.processWindow(window_info, list(self.current_tuples))
    for tup in self.current_tuples:
      self.ack(tup)
    self.current_tuples.clear()
