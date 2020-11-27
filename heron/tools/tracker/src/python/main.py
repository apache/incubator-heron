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

''' main.py '''
import logging
import os
import signal
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define
from tornado.httpclient import AsyncHTTPClient

from heron.tools.common.src.python.utils import config as common_config
from heron.common.src.python.utils import log
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import handlers
from heron.tools.tracker.src.python import utils
from heron.tools.tracker.src.python.config import Config, STATEMGRS_KEY
from heron.tools.tracker.src.python.tracker import Tracker

import click

Log = log.Log

class Application(tornado.web.Application):
  """ Tornado server application """
  def __init__(self, config):

    AsyncHTTPClient.configure(None, defaults=dict(request_timeout=120.0))
    self.tracker = Tracker(config)
    self.tracker.synch_topologies()
    tornadoHandlers = [
        (r"/topologies/containerfiledata", handlers.ContainerFileDataHandler,
         {"tracker":self.tracker}),
        (r"/topologies/containerfiledownload", handlers.ContainerFileDownloadHandler,
         {"tracker":self.tracker}),
        (r"/topologies/containerfilestats",
         handlers.ContainerFileStatsHandler, {"tracker":self.tracker}),
        # Deprecated. See https://github.com/apache/incubator-heron/issues/1754
         {"tracker":self.tracker}),
        (r"/topologies/runtimestate", handlers.RuntimeStateHandler, {"tracker":self.tracker}),
        (r"/topologies/exceptions", handlers.ExceptionHandler, {"tracker":self.tracker}),
        (r"/topologies/exceptionsummary", handlers.ExceptionSummaryHandler,
         {"tracker":self.tracker}),
        (r"/topologies/pid", handlers.PidHandler, {"tracker":self.tracker}),
        (r"/topologies/jstack", handlers.JstackHandler, {"tracker":self.tracker}),
        (r"/topologies/jmap", handlers.JmapHandler, {"tracker":self.tracker}),
        (r"/topologies/histo", handlers.MemoryHistogramHandler, {"tracker":self.tracker}),
    ]

    settings = dict(
        debug=True,
        serve_traceback=True,
        static_path=os.path.dirname(__file__)
    )
    tornado.web.Application.__init__(self, tornadoHandlers, **settings)
    Log.info("Tracker has started")

  def stop(self):
    self.tracker.stop_sync()


def define_options(port: int, config_file: str) -> None:
  """ define Tornado global variables """
  define("port", default=port)
  define("config_file", default=config_file)


def create_tracker_config(config_file: str, stmgr_override: dict) -> dict:
  # try to parse the config file if we find one
  config = utils.parse_config_file(config_file)
  if config is None:
    Log.debug(f"Config file does not exists: {config_file}")
    config = {STATEMGRS_KEY:[{}]}

  # update non-null options
  config[STATEMGRS_KEY][0].update(
      (k, v)
      for k, v in stmgr_override.items()
      if v is not None
  )
  return config


def show_version(_, __, value):
  if value:
    common_config.print_build_info()
    sys.exit(0)


@click.command()
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=show_version,
)
@click.option('--verbose', is_flag=True)
@click.option(
    '--config-file',
    help="path to a tracker config file",
    default=os.path.join(utils.get_heron_tracker_conf_dir(), constants.DEFAULT_CONFIG_FILE),
    show_default=True,
)
@click.option(
    '--port',
    type=int,
    default=constants.DEFAULT_PORT,
    show_default=True,
    help="local port to serve on",
)
@click.option(
    '--type',
    "stmgr_type",
    help=f"statemanager type e.g. {constants.DEFAULT_STATE_MANAGER_TYPE}",
    type=click.Choice(choices=["file", "zookeeper"]),
)
@click.option(
    '--name',
    help=f"statemanager name e.g. {constants.DEFAULT_STATE_MANAGER_NAME}",
)
@click.option(
    '--rootpath',
    help=f"statemanager rootpath e.g. {constants.DEFAULT_STATE_MANAGER_ROOTPATH}",
)
@click.option(
    '--tunnelhost',
    help=f"statemanager tunnelhost e.g. {constants.DEFAULT_STATE_MANAGER_TUNNELHOST}",
)
@click.option(
    '--hostport',
    help=f"statemanager hostport e.g. {constants.DEFAULT_STATE_MANAGER_HOSTPORT}",
)
def cli(
    config_file: str,
    stmgr_type: str,
    name: str,
    rootpath: str,
    tunnelhost: str,
    hostport: str,
    port: int,
    verbose: bool,
) -> None:
  """
  A HTTP service for serving data about clusters.

  The statemanager's config from the given config file can be overrided using
  options on this executable.

  """

  log.configure(logging.DEBUG if verbose else logging.INFO)

  # set Tornado global option
  define_options(port, config_file)

  stmgr_override = {
      "type": stmgr_type,
      "name": name,
      "rootpath": rootpath,
      "tunnelhost": tunnelhost,
      "hostport": hostport,
  }
  config = Config(create_tracker_config(config_file, stmgr_override))

  # create Tornado application
  application = Application(config)

  # pylint: disable=unused-argument
  # SIGINT handler:
  # 1. stop all the running zkstatemanager and filestatemanagers
  # 2. stop the Tornado IO loop
  def signal_handler(signum, frame):
    # start a new line after ^C character because this looks nice
    print('\n', end='')
    application.stop()
    tornado.ioloop.IOLoop.instance().stop()

  # associate SIGINT and SIGTERM with a handler
  signal.signal(signal.SIGINT, signal_handler)
  signal.signal(signal.SIGTERM, signal_handler)

  Log.info("Running on port: %d", port)
  if config_file:
    Log.info("Using config file: %s", config_file)
  Log.info(f"Using state manager:\n{config}")

  http_server = tornado.httpserver.HTTPServer(application)
  http_server.listen(port)

  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  cli() # pylint: disable=no-value-for-parameter
