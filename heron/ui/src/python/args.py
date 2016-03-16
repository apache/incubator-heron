import sys
import argparse

# default parameter - port for the web to ui to listen on
DEFAULT_PORT = 8889

# default parameter - url to connect to heron tracker
DEFAULT_TRACKER_URL = "http://localhost:8888"

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
  parser.add_argument(
      '--tracker_url',
      metavar='(a url; path to tracker; default: "' + DEFAULT_TRACKER_URL + '")',
      default=DEFAULT_TRACKER_URL)

  parser.add_argument(
      '--port',
      metavar='(an integer; port to listen; default: ' + str(DEFAULT_PORT) + ')',
      type = int, 
      default=DEFAULT_PORT)

  return parser

def create_parsers():
  parser = argparse.ArgumentParser(
      epilog = 'For detailed documentation, go to http://go/heron',
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
