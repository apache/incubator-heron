"""packer cli building module"""
import argparse

from heron.packer.src.python.common import constants
from heron.common.src.python.color import Log

help_epilog = '''Getting more help:
  heron-package help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  """Customized sub-command formatter"""
  # pylint: disable=protected-access
  def _format_action(self, action):
    parts = super(SubcommandHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts

def get_subparser(parser, command):
  """find subparser from 'parser' with name as 'command'"""
  # pylint: disable=protected-access
  subparsers_actions = [
      action for action in parser._actions
      if isinstance(action, argparse._SubParsersAction)
  ]

  # there will probably only be one subparser_action,
  # but better save than sorry
  for subparsers_action in subparsers_actions:
    # get all subparsers
    for choice, subparser in subparsers_action.choices.items():
      if choice == command:
        return subparser

  return None

def help_cmd(parser, namespace):
  """help command implementation"""
  command_help = namespace['help-command']

  if command_help == 'help':
    parser.print_help()
    return True

  subparser = get_subparser(parser, command_help)
  if subparser:
    print subparser.format_help()
    return True
  else:
    Log.error("Unknown subcommand \'%s\'" % command_help)
    return False

def get_commands(packer):
  """Bind cli commands with actual methods"""
  PACKER_COMMANDS = {
      "add_version": packer.add_version,
      "download": packer.download,
      "delete_version": packer.delete_version,
      "list_packages": packer.list_packages,
      "list_versions": packer.list_versions,
      "show_version": packer.show_version,
      "show_live": packer.show_live,
      "show_latest": packer.show_latest,
      "set_live": packer.set_live,
      "unset_live": packer.unset_live,
  }

  return PACKER_COMMANDS

def create_add_version_parser(parsers):
  """Add add_version cmd to parser"""
  add_version_parser = parsers.add_parser('add_version',
                                          help='Upload a topology package',
                                          add_help=False)
  add_version_parser.add_argument(constants.ROLE, help='role name')
  add_version_parser.add_argument(constants.PKG, help='package name')
  add_version_parser.add_argument('--%s' % constants.EXTRA,
                                  dest=constants.EXTRA,
                                  default='',
                                  help='extra user defined fields; separated by \\')
  add_version_parser.add_argument(constants.PKG_PATH, help='jar or tar filepath')
  add_version_parser.add_argument(constants.DESC, help='simple description about the jar')
  add_version_parser.set_defaults(command='add_version')

def create_download_parser(parsers):
  """Add download cmd to parser"""
  download_parser = parsers.add_parser('download',
                                       help='Download a topology package',
                                       add_help=False)
  download_parser.add_argument(constants.ROLE, help='role name')
  download_parser.add_argument(constants.PKG, help='packag name')
  download_parser.add_argument(constants.VERSION, help='version number or LIVE, LATEST tag')
  download_parser.add_argument('--%s' % constants.EXTRA,
                               dest=constants.EXTRA,
                               default='',
                               help='extra user defined fields; separated by \\')
  download_parser.add_argument(constants.DEST_PATH, help='download file path')
  download_parser.set_defaults(command='download')

def create_delete_version_parser(parsers):
  """Add delete_version cmd to parser"""
  delete_version_parser = parsers.add_parser('delete_version',
                                             help='Delete a topology pacakge',
                                             add_help=False)
  delete_version_parser.add_argument(constants.ROLE, help='role name')
  delete_version_parser.add_argument(constants.PKG, help='packag name')
  delete_version_parser.add_argument(constants.VERSION, help='version number or LIVE, LATEST tag')
  delete_version_parser.add_argument('--%s' % constants.EXTRA,
                                     dest=constants.EXTRA,
                                     default='',
                                     help='extra user defined fields; separated by \\')
  delete_version_parser.set_defaults(command='delete_version')

def create_list_packages_parser(parsers):
  """Add list_packages cmd to parser"""
  list_packages_parser = parsers.add_parser('list_packages',
                                            help="List packages owned by a role",
                                            add_help=False)
  list_packages_parser.add_argument(constants.ROLE, help='role name')
  list_packages_parser.add_argument('--%s' % constants.EXTRA,
                                    dest=constants.EXTRA,
                                    default='',
                                    help='extra user defined fields; separated by \\')
  list_packages_parser.add_argument('--%s' % constants.RAW,
                                    dest=constants.RAW,
                                    action='store_true',
                                    help='print the result in raw format')
  list_packages_parser.set_defaults(command='list_packages')

def create_list_versions_parser(parsers):
  """Add list_versions cmd to parser"""
  list_versions_parser = parsers.add_parser('list_versions',
                                            help="List a package's versions",
                                            add_help=False)
  list_versions_parser.add_argument(constants.ROLE, help='role name')
  list_versions_parser.add_argument(constants.PKG, help='pacakge name')
  list_versions_parser.add_argument('--%s' % constants.EXTRA,
                                    dest=constants.EXTRA,
                                    default='',
                                    help='extra user defined fields; separated by \\')
  list_versions_parser.add_argument('--%s' % constants.RAW,
                                    dest=constants.RAW,
                                    action='store_true',
                                    help='print the result in raw format')
  list_versions_parser.set_defaults(command='list_versions')

def create_show_version_parser(parsers):
  """Add show_version cmd to parser"""
  show_version_parser = parsers.add_parser('show_version',
                                           help="Show information for package's chosen version",
                                           add_help=False)
  show_version_parser.add_argument(constants.ROLE, help='role')
  show_version_parser.add_argument(constants.PKG, help='package')
  show_version_parser.add_argument(constants.VERSION, help='version number or LIVE, LATEST tag')
  show_version_parser.add_argument('--%s' % constants.EXTRA,
                                   dest=constants.EXTRA, default='',
                                   help='extra user defined fields; separated by \\')
  show_version_parser.add_argument('--%s' % constants.RAW,
                                   dest=constants.RAW,
                                   action='store_true',
                                   help='print the result in raw format')
  show_version_parser.set_defaults(command='show_version')

def create_show_live_parser(parsers):
  """Add show_live cmd to parser"""
  show_live_parser = parsers.add_parser('show_live',
                                        help="Show information for package's live version",
                                        add_help=False)
  show_live_parser.add_argument(constants.ROLE, help='role')
  show_live_parser.add_argument(constants.PKG, help='package')
  show_live_parser.add_argument('--%s' % constants.EXTRA,
                                dest=constants.EXTRA,
                                default='',
                                help='extra user defined fields; separated by \\')
  show_live_parser.add_argument('--%s' % constants.RAW,
                                dest=constants.RAW,
                                action='store_true',
                                help='print the result in raw format')
  show_live_parser.set_defaults(command='show_live')

def create_show_latest_parser(parsers):
  """Add show_latest cmd to parser"""
  show_latest_parser = parsers.add_parser('show_latest',
                                          help="Show information for pacakge's latest version",
                                          add_help=False)
  show_latest_parser.add_argument(constants.ROLE, help='role')
  show_latest_parser.add_argument(constants.PKG, help='package')
  show_latest_parser.add_argument('--%s' % constants.EXTRA,
                                  dest=constants.EXTRA,
                                  default='',
                                  help='extra user defined fields; separated by \\')
  show_latest_parser.add_argument('--%s' % constants.RAW,
                                  dest=constants.RAW,
                                  help='print the result in raw format')
  show_latest_parser.set_defaults(command='show_latest')

def create_set_live_parser(parsers):
  """Add set_live cmd to parser"""
  set_live_parser = parsers.add_parser('set_live',
                                       help="Set 'live' tag on a package's version",
                                       add_help=False)
  set_live_parser.add_argument(constants.ROLE, help='role name')
  set_live_parser.add_argument(constants.PKG, help='packag name')
  set_live_parser.add_argument(constants.VERSION, help='version number or LIVE, LATEST tag')
  set_live_parser.add_argument('--%s' % constants.EXTRA,
                               dest=constants.EXTRA,
                               default='',
                               help='extra user defined fields; separated by \\')
  set_live_parser.set_defaults(command='set_live')

def create_unset_live_parser(parsers):
  """Add unset_live cmd to parser"""
  unset_live_parser = parsers.add_parser('unset_live',
                                         help="Unset 'live' tag on a pacakge's version",
                                         add_help=False)
  unset_live_parser.add_argument(constants.ROLE, help='role name')
  unset_live_parser.add_argument(constants.PKG, help='packag name')
  unset_live_parser.add_argument('--%s' % constants.EXTRA,
                                 dest=constants.EXTRA,
                                 default='',
                                 help='extra user defined fields; separated by \\')
  unset_live_parser.set_defaults(command='unset_live')

def create_help_parser(parsers):
  """Add help cmd to parser"""
  help_parser = parsers.add_parser('help', help='Print help for commands', add_help=False)
  help_parser.add_argument('help-command',
                           nargs='?',
                           default='help',
                           help='Provide help for a command')
  help_parser.set_defaults(command='help')
  # pylint: disable=protected-access
  help_parser._positionals.title = "Required arguments"
  help_parser._optionals.title = "Optional arguments"

def create_parser():
  """Create a command line parser for heron package management tool"""
  parser = argparse.ArgumentParser(prog="heron-package",
                                   epilog=help_epilog,
                                   formatter_class=SubcommandHelpFormatter,
                                   add_help=False)
  subparsers = parser.add_subparsers(title="Available commands", metavar='<command> <options>')

  create_add_version_parser(subparsers)
  create_download_parser(subparsers)
  create_delete_version_parser(subparsers)
  create_list_packages_parser(subparsers)
  create_list_versions_parser(subparsers)
  create_show_version_parser(subparsers)
  create_show_live_parser(subparsers)
  create_show_latest_parser(subparsers)
  create_set_live_parser(subparsers)
  create_unset_live_parser(subparsers)
  create_help_parser(subparsers)

  return parser
