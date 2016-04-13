import argparse
import os
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web

from tornado.escape import json_encode, utf8
from tornado.options import define, options

from heron.tracker.src.python import constants
from heron.tracker.src.python import handlers
from heron.tracker.src.python import log
from heron.tracker.src.python.log import Log as LOG
from heron.tracker.src.python import utils
from heron.tracker.src.python.tracker import Tracker

class Application(tornado.web.Application):
  def __init__(self):
    tracker = Tracker()
    self.tracker = tracker
    tracker.synch_topologies(options.config_file)
    tornadoHandlers = [
      (r"/", handlers.MainHandler),
      (r"/topologies", handlers.TopologiesHandler, {"tracker":tracker}),
      (r"/topologies/states", handlers.StatesHandler, {"tracker":tracker}),
      (r"/topologies/info", handlers.TopologyHandler, {"tracker":tracker}),
      (r"/topologies/logicalplan", handlers.LogicalPlanHandler, {"tracker":tracker}),
      (r"/topologies/logfiledata", handlers.LogfileDataHandler, {"tracker":tracker}),
      (r"/topologies/physicalplan", handlers.PhysicalPlanHandler, {"tracker":tracker}),
      (r"/topologies/executionstate", handlers.ExecutionStateHandler, {"tracker":tracker}),
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
      debug = True,
      serve_traceback = True,
      static_path = os.path.dirname(__file__)
    )
    tornado.web.Application.__init__(self, tornadoHandlers, **settings)


class _HelpAction(argparse._HelpAction):
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
        print("Subparser '{}'".format(choice))
        print(subparser.format_help())

    parser.exit()

class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  def _format_action(self, action):
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts

def add_titles(parser):
  parser._positionals.title = "Required arguments"
  parser._optionals.title = "Optional arguments"
  return parser

def add_arguments(parser):
  default_config_file = os.path.join(
      utils.get_heron_tracker_conf_dir(), constants.DEFAULT_CONFIG_FILE)

  parser.add_argument(
      '--config-file',
      metavar='(a string; path to config file; default: "' + default_config_file + '")',
      default=default_config_file)

  parser.add_argument(
      '--port',
      metavar='(an integer; port to listen; default: ' + str(constants.DEFAULT_PORT) + ')',
      type = int, 
      default=constants.DEFAULT_PORT)

  return parser

def create_parsers():
  parser = argparse.ArgumentParser(
      epilog = 'For detailed documentation, go to http://github.com/twitter/heron',
      usage = "%(prog)s [options] [help]",
      add_help = False)

  parser = add_titles(parser)
  parser = add_arguments(parser)

  ya_parser = argparse.ArgumentParser(
      parents = [parser],
      formatter_class=SubcommandHelpFormatter,
      add_help = False)

  subparsers = ya_parser.add_subparsers(
      title = "Available commands")
  
  help_parser = subparsers.add_parser(
      'help',
      help='Prints help',
      add_help = False)

  help_parser.set_defaults(help=True)
  return (parser, ya_parser)

def define_options(port, config_file):
  define("port", default=port)
  define("config_file", default=config_file)

def main():
  log.configure(log.logging.DEBUG)

  # create the parser and parse the arguments
  (parser, ya_parser) = create_parsers()
  (args, remaining) = parser.parse_known_args()

  if remaining:
    yaargs = ya_parser.parse_args(args = remaining, namespace=args)
    parser.print_help()
    parser.exit()

  namespace = vars(args)
  LOG.info("Running on port: %d", namespace['port'])
  LOG.info("Using config file: %s", namespace['config_file'])

  # TO DO check if the config file exists

  define_options(namespace['port'], namespace['config_file'])
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(namespace['port'])
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()
