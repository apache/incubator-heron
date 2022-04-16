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
import signal
import logging
import os
import sys

import click
import uvicorn

from heron.tools.common.src.python.utils import config as common_config
from heron.common.src.python.utils import log
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import utils
from heron.tools.tracker.src.python.config import Config, STATEMGRS_KEY
from heron.tools.tracker.src.python.tracker import Tracker
from heron.tools.tracker.src.python.app import app
from heron.tools.tracker.src.python import state

Log = log.Log
Log.setLevel(logging.DEBUG)

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

  log_level = logging.DEBUG if verbose else logging.INFO
  log.configure(log_level)

  stmgr_override = {
      "type": stmgr_type,
      "name": name,
      "rootpath": rootpath,
      "tunnelhost": tunnelhost,
      "hostport": hostport,
  }
  config = Config(create_tracker_config(config_file, stmgr_override))

  state.tracker = Tracker(config)
  state.tracker.sync_topologies()
  # this only returns when interrupted
  uvicorn.run(app, host="0.0.0.0", port=port, log_level=log_level)
  state.tracker.stop_sync()

  # non-daemon threads linger and stop the process for quitting, so signal
  # for cleaning up
  os.kill(os.getpid(), signal.SIGKILL)


if __name__ == "__main__":
  cli() # pylint: disable=no-value-for-parameter
