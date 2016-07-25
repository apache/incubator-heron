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

#!/usr/bin/env python2.7
''' main.py '''
import argparse
import logging
import os
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web

from tornado.options import define, options
from heron.tracker.src.python import constants
from heron.tracker.src.python import handlers
from heron.tracker.src.python import utils
from heron.tracker.src.python.config import Config
from heron.tracker.src.python.tracker import Tracker
import heron.common.src.python.utils as common_utils

LOG = logging.getLogger(__name__)
from heron.common.src.python.color import Log

class Application(tornado.web.Application):
  """ Tornado server application """
  def __init__(self):

    config = Config(options.config_file)
    tracker = Tracker(config)
    self.tracker = tracker
    tracker.synch_topologies()
    tornadoHandlers = [
        (r"/", handlers.MainHandler),
        (r"/clusters", handlers.ClustersHandler, {"tracker":tracker}),
        (r"/topologies", handlers.TopologiesHandler, {"tracker":tracker}),
        (r"/topologies/states", handlers.StatesHandler, {"tracker":tracker}),
        (r"/topologies/info", handlers.TopologyHandler, {"tracker":tracker}),
        (r"/topologies/logicalplan", handlers.LogicalPlanHandler, {"tracker":tracker}),
        (r"/topologies/containerfiledata", handlers.ContainerFileDataHandler, {"tracker":tracker}),
        (r"/topologies/containerfilestats",
         handlers.ContainerFileStatsHandler, {"tracker":tracker}),
        (r"/topologies/physicalplan", handlers.PhysicalPlanHandler, {"tracker":tracker}),
        (r"/topologies/executionstate", handlers.ExecutionStateHandler, {"tracker":tracker}),
        (r"/topologies/schedulerlocation", handlers.SchedulerLocationHandler, {"tracker":tracker}),
        (r"/topologies/metrics", handlers.MetricsHandler, {"tracker":tracker}),
        (r"/topologies/metricstimeline", handlers.MetricsTimelineHandler, {"tracker":tracker}),
        (r"/topologies/metricsquery", handlers.MetricsQueryHandler, {"tracker":tracker}),
        (r"/topologies/exceptions", handlers.ExceptionHandler, {"tracker":tracker}),
        (r"/topologies/exceptionsummary", handlers.ExceptionSummaryHandler, {"tracker":tracker}),
        (r"/machines", handlers.MachinesHandler, {"tracker":tracker}),
        (r"/topologies/pid", handlers.PidHandler, {"tracker":tracker}),
        (r"/topologies/jstack", handlers.JstackHandler, {"tracker":tracker}),
        (r"/topologies/jmap", handlers.JmapHandler, {"tracker":tracker}),
        (r"/topologies/histo", handlers.MemoryHistogramHandler, {"tracker":tracker}),
        (r"(.*)", handlers.DefaultHandler),
    ]

    settings = dict(
        debug=True,
        serve_traceback=True,
        static_path=os.path.dirname(__file__)
    )
    tornado.web.Application.__init__(self, tornadoHandlers, **settings)
    Log.info("-" * 100)
    Log.info("Tracker started")
    Log.info("-" * 100)


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
      for choice, subparser in subparsers_action.choices.items():
        print "Subparser '{}'".format(choice)
        print subparser.format_help()

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
      epilog='For detailed documentation, go to http://github.com/twitter/heron',
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

def configure_logging(level):
  Log.setLevel(level)

def main():
  """ main """
  # create the parser and parse the arguments
  (parser, _) = create_parsers()
  (args, remaining) = parser.parse_known_args()

  if remaining == ['help']:
    parser.print_help()
    parser.exit()

  elif remaining == ['version']:
    common_utils.print_version()
    parser.exit()

  elif remaining != []:
    LOG.error('Invalid subcommand')
    sys.exit(1)

  namespace = vars(args)

  if namespace["verbose"]:
    configure_logging(logging.DEBUG)
  else:
    configure_logging(logging.INFO)

  Log.info("Running on port: %d", namespace['port'])
  Log.info("Using config file: %s", namespace['config_file'])

  # TO DO check if the config file exists

  define_options(namespace['port'], namespace['config_file'])
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(namespace['port'])
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
