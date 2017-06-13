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
import colorlog

# Create the logger
# pylint: disable=invalid-name
logging.basicConfig()
Log = logging.getLogger()

# time formatter - date - time - UTC offset
# e.g. "08/16/1988 21:30:00 +1030"
# see time formatter documentation for more
date_format = "%Y-%m-%d %H:%M:%S %z"

def configure(level=logging.INFO, logfile=None):
  """ Configure logger which dumps log on terminal

  :param level: logging level: info, warning, verbose...
  :type level: logging level
  :param logfile: log file name, default to None
  :type logfile: string
  :return: None
  :rtype: None
  """

  # Remove all the existing StreamHandlers to avoid duplicate
  for handler in Log.handlers:
    if isinstance(handler, logging.StreamHandler):
      Log.handlers.remove(handler)

  Log.setLevel(level)

  # if logfile is specified, FileHandler is used
  if logfile is not None:
    log_format = "[%(asctime)s] [%(levelname)s]: %(message)s"
    formatter = logging.Formatter(fmt=log_format, datefmt=date_format)
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(formatter)
    Log.addHandler(file_handler)
  # otherwise, use StreamHandler to output to stream (stdout, stderr...)
  else:
    log_format = "[%(asctime)s] %(log_color)s[%(levelname)s]%(reset)s: %(message)s"
    # pylint: disable=redefined-variable-type
    formatter = colorlog.ColoredFormatter(fmt=log_format, datefmt=date_format)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    Log.addHandler(stream_handler)


def init_rotating_logger(level, logfile, max_files, max_bytes):
  """Initializes a rotating logger

  It also makes sure that any StreamHandler is removed, so as to avoid stdout/stderr
  constipation issues
  """
  logging.basicConfig()

  root_logger = logging.getLogger()
  log_format = "[%(asctime)s] [%(levelname)s] %(filename)s: %(message)s"

  root_logger.setLevel(level)
  handler = RotatingFileHandler(logfile, maxBytes=max_bytes, backupCount=max_files)
  handler.setFormatter(logging.Formatter(fmt=log_format, datefmt=date_format))
  root_logger.addHandler(handler)

  for handler in root_logger.handlers:
    root_logger.debug("Associated handlers - " + str(handler))
    if isinstance(handler, logging.StreamHandler):
      root_logger.debug("Removing StreamHandler: " + str(handler))
      root_logger.handlers.remove(handler)

def set_logging_level(cl_args):
  """simply set verbose level based on command-line args

  :param cl_args: CLI arguments
  :type cl_args: dict
  :return: None
  :rtype: None
  """
  if 'verbose' in cl_args and cl_args['verbose']:
    configure(logging.DEBUG)
  else:
    configure(logging.INFO)
