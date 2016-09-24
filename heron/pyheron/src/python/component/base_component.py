# Copyright 2016 - Parsely, Inc. (d/b/a Parse.ly)
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
'''base_component.py'''

class NotCompatibleError(RuntimeError):
  """Method is not compatible in PyHeron"""
  pass

class BaseComponent(object):
  """Base component for PyHeron spout and bolt"""
  def __init__(self, delegate):
    """Initializes BaseComponent

    :param delegate: SpoutInstance or BoltInstance
    """
    self.delegate = delegate
    self.logger = self.delegate.logger

  def log(self, message, level=None):
    """Log message, optionally providing a logging level

    It is compatible with StreamParse API.

    :type message: str
    :param message: the log message to send
    :type level: str
    :param level: the logging level,
                  one of: trace (=debug), debug, info, warn or error (default: info)
    """
    self.delegate.log(message, level)

  # pylint: disable=unused-argument
  # pylint: disable=no-self-use
  @staticmethod
  def is_heartbeat(tup):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("is_heartbeat() method is not applicable in Heron.")

  def raise_exception(self, exception, tup=None):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("raise_exception() method is not applicable in Heron.")

  def read_handshake(self):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("read_handshake() method is not applicable in Heron.")

  def read_message(self):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("read_message() method is not applicable in Heron.")

  def report_metric(self, name, value):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("report_metric() method is not applicable in Heron.\n"
                             "Please use a metrics collector instead.")

  def run(self):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("run() method is not applicable in Heron.")

  def send_message(self, message):
    """Streamparse API that is not applicable for PyHeron"""
    raise NotCompatibleError("send_message() method is not applicable in Heron.")
