import os
import sys
import argparse
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.escape import json_encode, utf8
from tornado.options import define, options

from heron.tracker.src.python import utils
from heron.tracker.src.python import handlers
from heron.tracker.src.python import log
from heron.tracker.src.python.log import Log as LOG
from heron.tracker.src.python.tracker import Tracker

define("stateconf", default='zkstateconf', help="Yaml config file without extension for state locations")
define("port", default=8888, type=int, help="HTTP port to run the Tracker")

class Application(tornado.web.Application):
  def __init__(self):
    tracker = Tracker()
    self.tracker = tracker
    tracker.synch_topologies(options.stateconf)
    tornadoHandlers = [
      (r"/", handlers.MainHandler),
      (r"/topologies", handlers.TopologiesHandler, {"tracker":tracker}),
      (r"/topologies/states", handlers.StatesHandler, {"tracker":tracker}),
      (r"/topologies/info", handlers.TopologyHandler, {"tracker":tracker}),
      (r"/topologies/logicalplan", handlers.LogicalPlanHandler, {"tracker":tracker}),
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

def add_config(parser):

  # the default config file path
  default_config_file = os.path.join(
      utils.get_heron_tracker_conf_dir(), "localfilestateconf.yaml")

  parser.add_argument(
      '--config-file',
      metavar='(a string; path to config file; default: "' + default_config_file + '")',
      default=default_config_file)

  parser.add_argument(
      '--port',
      metavar='(an integer; port to listen; default: 8888)',
      type = int, 
      default=8888)

  return parser

def create_parsers():
  parser = argparse.ArgumentParser(
      epilog = 'For detailed documentation, go to http://go/heron',
      usage = "%(prog)s [options] <command>",
      formatter_class=SubcommandHelpFormatter,
      add_help = False)

  parser = add_titles(parser)
  parser = add_config(parser)

  ya_parser = argparse.ArgumentParser(parents = [parser], add_help = False)
  subparsers = ya_parser.add_subparsers(
      title = "Available commands",
      # metavar = 'help          Prints help')
      metavar = '<command> [command-options]')

  help_parser = subparsers.add_parser(
      'help',
      help='Prints help',
      add_help = False)

  help_parser.set_defaults(help=True)
  return (parser, ya_parser)

def main():
  log.configure(log.logging.DEBUG)

  # create the parser and parse the arguments
  # (parser, help_parser) = create_parsers()
  
  # (args, remaining) = parser.parse_known_args()

  # if remaining:
  #   yaargs = help_parser.parse_args(args = remaining, namespace=args)
  #   print yaargs
  # else:
  #   print args
  # sys.exit(0) 

  options.parse_command_line()
  port = options.port
  LOG.info("Running on port: " + str(port))
  http_server = tornado.httpserver.HTTPServer(Application())
  http_server.listen(port)
  tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
  main()

