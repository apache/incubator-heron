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
''' log.py '''
import logging
from logging.handlers import RotatingFileHandler

# Create the logger
# pylint: disable=invalid-name
Log = logging.getLogger('common')

def configure(level, logfile=None):
  '''
  :param level:
  :param logfile:
  :return:
  '''
  log_format = "%(asctime)s-%(levelname)s: %(message)s"
  date_format = '%a, %d %b %Y %H:%M:%S'

  logging.basicConfig(format=log_format, datefmt=date_format)
  Log.setLevel(level)

  if logfile is not None:
    handle = logging.FileHandler(logfile)
    handle.setFormatter(logging.Formatter(log_format))
    Log.addHandler(handle)

def init_rotating_logger(level, logfile, max_files, max_bytes):
  """Initializes a rotating logger

  It also makes sure that any StreamHandler is removed, so as to avoid stdout/stderr
  constipation issues
  """
  root_logger = logging.getLogger()
  log_format = "%(asctime)s:%(levelname)s:%(filename)s: %(message)s"

  root_logger.setLevel(level)
  handler = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=max_files)
  handler.setFormatter(logging.Formatter(fmt=log_format))
  root_logger.addHandler(handler)

  for handler in root_logger.handlers:
    root_logger.debug("Associated handlers - " + str(handler))
    if isinstance(handler, logging.StreamHandler):
      root_logger.debug("Removing StreamHandler: " + str(handler))
      root_logger.handlers.remove(handler)
