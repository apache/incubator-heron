#!/usr/bin/env python2.7
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
from __future__ import print_function

import argparse
import os
import signal
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import define
from tornado.httpclient import AsyncHTTPClient

import heron.tools.common.src.python.utils.config as common_config
import heron.common.src.python.utils.log as log
from heron.tools.tracker.src.python import constants
from heron.tools.tracker.src.python import handlers
from heron.tools.tracker.src.python import utils
from heron.tools.tracker.src.python.config import Config, STATEMGRS_KEY
from heron.tools.tracker.src.python.tracker import Tracker

Log = log.Log

class Application(tornado.web.Application):
  """ Tornado server application """
  def __init__(self, config):

    AsyncHTTPClient.configure(None, defaults=dict(request_timeout=120.0))
    self.tracker = Tracker(config)
    self.tracker.synch_topologies()
    tornadoHandlers = [
        (r"/", handlers.MainHandler),
        (r"/clusters", handlers.ClustersHandler, {"tracker":self.tracker}),
        (r"/topologies", handlers.TopologiesHandler, {"tracker":self.tracker}),
        (r"/topologies/states", handlers.StatesHandler, {"tracker":self.tracker}),
        (r"/topologies/info", handlers.TopologyHandler, {"tracker":self.tracker}),
        (r"/topologies/logicalplan", handlers.LogicalPlanHandler, {"tracker":self.tracker}),
        (r"/topologies/config", handlers.TopologyConfigHandler, {"tracker":self.tracker}),
        (r"/topologies/containerfiledata", handlers.ContainerFileDataHandler,
         {"tracker":self.tracker}),
        (r"/topologies/containerfiledownload", handlers.ContainerFileDownloadHandler,
         {"tracker":self.tracker}),
        (r"/topologies/containerfilestats",
         handlers.ContainerFileStatsHandler, {"tracker":self.tracker}),
        (r"/topologies/physicalplan", handlers.PhysicalPlanHandler, {"tracker":self.tracker}),
        (r"/topologies/packingplan", handlers.PackingPlanHandler, {"tracker":self.tracker}),
        # Deprecated. See https://github.com/apache/incubator-heron/issues/1754
        (r"/topologies/executionstate", handlers.ExecutionStateHandler, {"tracker":self.tracker}),
        (r"/topologies/schedulerlocation", handlers.SchedulerLocationHandler,
         {"tracker":self.tracker}),
        (r"/topologies/metadata", handlers.MetaDataHandler, {"tracker":self.tracker}),
        (r"/topologies/runtimestate", handlers.RuntimeStateHandler, {"tracker":self.tracker}),
        (r"/topologies/metrics", handlers.MetricsHandler, {"tracker":self.tracker}),
        (r"/topologies/metricstimeline", handlers.MetricsTimelineHandler, {"tracker":self.tracker}),
        (r"/topologies/metricsquery", handlers.MetricsQueryHandler, {"tracker":self.tracker}),
        (r"/topologies/exceptions", handlers.ExceptionHandler, {"tracker":self.tracker}),
        (r"/topologies/exceptionsummary", handlers.ExceptionSummaryHandler,
         {"tracker":self.tracker}),
        (r"/machines", handlers.MachinesHandler, {"tracker":self.tracker}),
        (r"/topologies/pid", handlers.PidHandler, {"tracker":self.tracker}),
        (r"/topologies/jstack", handlers.JstackHandler, {"tracker":self.tracker}),
        (r"/topologies/jmap", handlers.JmapHandler, {"tracker":self.tracker}),
        (r"/topologies/histo", handlers.MemoryHistogramHandler, {"tracker":self.tracker}),
        (r"(.*)", handlers.DefaultHandler),
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

# pylint: disable=protected-access
class _HelpAction(argparse._HelpAction):
  """ HelpAction """
  def __call__(self, parser, namespace, values, option_string=None):
    parser.print_help()

    # retrieve subparsers from parser
    subparsers_actions = [
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)]

    # there will probably only be one subparser_action,
    # but better save than sorry
    for subparsers_action in subparsers_actions:
      # get all subparsers and print help
      for choice, subparser in list(subparsers_action.choices.items()):
        print("Subparser '{}'".format(choice))
        print(subparser.format_help())

    parser.exit()

# pylint: disable=bad-super-call
class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  """ Subcommand help formatter """
  def _format_action(self, action):
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts


def add_titles(parser):
  """ add titles """
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser


def add_arguments(parser):
  """ add arguments """
  default_config_file = os.path.join(
      utils.get_heron_tracker_conf_dir(), constants.DEFAULT_CONFIG_FILE)

  parser.add_argument(
      '--config-file',
      metavar='(a string; path to config file; default: "' + default_config_file + '")',
      default=default_config_file)

  parser.add_argument(
      '--type',
      metavar='(an string; type of state manager (zookeeper or file, etc.); example: ' \
        + str(constants.DEFAULT_STATE_MANAGER_TYPE) + ')',
      choices=["file", "zookeeper"])

  parser.add_argument(
      '--name',
      metavar='(an string; name to be used for the state manager; example: ' \
        + str(constants.DEFAULT_STATE_MANAGER_NAME) + ')')

  parser.add_argument(
      '--rootpath',
      metavar='(an string; where all the states are stored; example: ' \
        + str(constants.DEFAULT_STATE_MANAGER_ROOTPATH) + ')')

  parser.add_argument(
      '--tunnelhost',
      metavar='(an string; if ssh tunneling needs to be established to connect to it; example: ' \
        + str(constants.DEFAULT_STATE_MANAGER_TUNNELHOST) + ')')

  parser.add_argument(
      '--hostport',
      metavar='(an string; only used to connect to zk, must be of the form \'host:port\';'\
      ' example: ' + str(constants.DEFAULT_STATE_MANAGER_HOSTPORT) + ')')

  parser.add_argument(
      '--port',
      metavar='(an integer; port to listen; default: ' + str(constants.DEFAULT_PORT) + ')',
      type=int,
      default=constants.DEFAULT_PORT)

  parser.add_argument(
      '--verbose',
      action='store_true')

  return parser

def create_parsers():
  """ create argument parser """
  parser = argparse.ArgumentParser(
      epilog='For detailed documentation, go to http://github.com/apache/incubator-heron',
      usage="%(prog)s [options] [help]",
      add_help=False)

  parser = add_titles(parser)
  parser = add_arguments(parser)

  ya_parser = argparse.ArgumentParser(
      parents=[parser],
      formatter_class=SubcommandHelpFormatter,
      add_help=False)

  subparsers = ya_parser.add_subparsers(
      title="Available commands")

  help_parser = subparsers.add_parser(
      'help',
      help='Prints help',
      add_help=False)

  help_parser.set_defaults(help=True)

  subparsers.add_parser(
      'version',
      help='Prints version',
      add_help=True)

  return parser, ya_parser

def define_options(port, config_file):
  """ define Tornado global variables """
  define("port", default=port)
  define("config_file", default=config_file)

def create_tracker_config(namespace):
  # try to parse the config file if we find one
  config_file = namespace["config_file"]
  config = utils.parse_config_file(config_file)
  if config is None:
    Log.debug("Config file does not exists: %s" % config_file)
    config = {STATEMGRS_KEY:[{}]}

  # update the config if we have any flags
  config_flags = ["type", "name", "rootpath", "tunnelhost", "hostport"]
  config_to_update = config[STATEMGRS_KEY][0]
  for flag in config_flags:
    value = namespace.get(flag, None)
    if value is not None:
      config_to_update[flag] = value

  return config

def main():
  """ main """
  # create the parser and parse the arguments
  (parser, _) = create_parsers()
  (args, remaining) = parser.parse_known_args()

  if remaining == ['help']:
    parser.print_help()
    parser.exit()

  elif remaining == ['version']:
    common_config.print_build_info()
    parser.exit()

  elif remaining != []:
    Log.error('Invalid subcommand')
    sys.exit(1)

  namespace = vars(args)

  log.set_logging_level(namespace)

  # set Tornado global option
  define_options(namespace['port'], namespace['config_file'])

  config = Config(create_tracker_config(namespace))

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

  Log.info("Running on port: %d", namespace['port'])
  if namespace["config_file"]:
    Log.info("Using config file: %s", namespace['config_file'])
  Log.info("Using state manager:\n" + str(config))

  http_server = tornado.httpserver.HTTPServer(application)
  http_server.listen(namespace['port'])

  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
