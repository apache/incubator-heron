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
''' argparser.py: HERON RC parser support for specifying config level arguments in an RC file.'''
import argparse
import collections
import json
import os
import re
import sys
import traceback
from heron.common.src.python.color import Log
import heron.common.src.python.utils.config as config


##########################################################################
# Run the command
##########################################################################

HERON_RC_FILE = "~/.heronrc"
HERON_RC = os.path.expanduser(HERON_RC_FILE)
HERON_RC_SPL = '@' + HERON_RC
# pylint: disable=anomalous-backslash-in-string
heron_command_pattern = re.compile('(^[^:]*):([^:]*):([^\s]*) (.*)')
filters = ['^@']
expressions = [re.compile(x) for x in filters]

help_epilog = '''Getting more help:
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''

class HeronRCArgumentParser(argparse.ArgumentParser):
  """
  HERON RC parser support for specifying config level arguments in an RC file.
  check README.md.
  """
  cmdmap = collections.defaultdict(dict)

  """
  HERON RC parser support for specifying config level arguments in an RC file.
  check README.md.
  """
  def __init__(self, *args, **kwargs):
    rcfile = HeronRCArgumentParser.getAndRemoveKey(kwargs, "rcfile")
    self.rccommand = HeronRCArgumentParser.getAndRemoveKey(
        kwargs, "rccommand")
    self.rcclusterrole = HeronRCArgumentParser.getAndRemoveKey(
        kwargs, "rcclusterrole")
    HeronRCArgumentParser.initializeFromRC(rcfile)
    super(HeronRCArgumentParser, self).__init__(*args, **kwargs)

  @classmethod
  def remove_comments(cls, string):
    pattern = r"(\#.*$)"
    # first group captures quoted strings (double or single)
    # second group captures comments (//single-line or /* multi-line */)
    regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

    def _replacer(match):
      # if the 2nd group (capturing comments) is not None,
      # it means we have captured a non-quoted (real) comment string.
      if match.group(1) is not None:
        return ""  # so we will return empty to remove the comment
      else:  # otherwise, we will return the 1st group
        return match.group(1)  # captured quoted-string
    return regex.sub(_replacer, string)

  @classmethod
  def getAndRemoveKey(cls, dictionary, key):
    val = None
    if key in dictionary:
      val = dictionary[key]
      del dictionary[key]
    return val

  # initialize the command map from heron rc file in the parser,
  # that can be later used for command substitution during parse_args phase
  # patterns

  @classmethod
  def initializeFromRC(cls, rcfile):
    if len(cls.cmdmap) > 0:
      return
    effective_rc = (rcfile, HERON_RC)[rcfile is None]
    Log.info('Effective RC file is %s', effective_rc)
    if os.path.exists(effective_rc):
      with open(effective_rc) as f:
        cls.cmdmap['*']['*'] = collections.defaultdict(dict)
        cls.cmdmap['*']['*']['*'] = ''
        for line in f:
          m = heron_command_pattern.match(line)
          app, value, command, env = '', '', '', ''
          if m is not None:
            value = cls.remove_comments(m.group(4).rstrip(os.linesep))
            app = (m.group(1), '')[m.group(1) is None or m.group(1) == '']
            command = (m.group(2), '')[m.group(2) is None or m.group(1) == '']
            env = (m.group(3), '')[m.group(3) is None or m.group(2) == '']
          else:
            continue
          # make sure that all the single args have a boolean value
          # associated so that we can load the args to a key value
          # structure
          args_list = config.insert_bool_values(value.split())
          args_list_string = ' '.join(args_list)
          if command is None or command == '':
            continue
          if app is None or app == '':
            continue
          if env is None or env == '':
            continue
          if app not in cls.cmdmap:
            cls.cmdmap[app] = collections.defaultdict(dict)

          if command in cls.cmdmap[app] and env in cls.cmdmap[app][command]:
            cls.cmdmap[app][command][env] = cls.cmdmap[app][command][env] + ' ' + args_list_string
          else:
            cls.cmdmap[app][command][env] = args_list_string
      Log.debug("RC cmdmap %s", json.dumps(cls.cmdmap))
    else:
      Log.warn("%s is not an existing file", effective_rc)

  # for each command / cluster-role-env combination, get the commands from heronrc
  # remove any duplicates that have already been supplied already  and
  # present in the command-dictionary
  @classmethod
  def get_args_for_command_role(cls, app, command, role):
    args_for_command_role = ''
    if app in cls.cmdmap and command in cls.cmdmap[app] and role in cls.cmdmap[app][command]:
      args_for_command_role = (cls.cmdmap[app][command][role],
                               args_for_command_role)[cls.cmdmap[app][command][role] is None]
    return args_for_command_role.split()

  # this is invoked when the parser.parse_args is called
  #  apply the commands in the following precedence order
  #  use the defaults in the command line

  def _read_args_from_files(self, arg_strings):
    new_arg_strings = []
    command = self.rccommand
    if len(sys.argv) > 1:
      command = (sys.argv[1], self.rccommand)[self.rccommand != '']
    role = self.rcclusterrole
    if len(sys.argv) > 2:
      role = (sys.argv[2], self.rcclusterrole)[self.rcclusterrole != '']
    app = self.prog
    new_arg_strings.extend(
        self.get_args_for_command_role(app, command, role))
    new_arg_strings.extend(
        self.get_args_for_command_role(app, command, '*'))
    new_arg_strings.extend(self.get_args_for_command_role(app, '*', '*'))
    new_arg_strings.extend(self.get_args_for_command_role('*', '*', '*'))
    arg_strings.extend(new_arg_strings)
    return arg_strings

  def parse_known_args(self, args=None, namespace=None):
    namespace, args = super(HeronRCArgumentParser,
                            self).parse_known_args(args, namespace)
    if self.prog == 'heron':
      try:
        for key in namespace.__dict__:
          val = namespace.__dict__[key]
          if val is not None and isinstance(val, list) and len(val) > 0:
            namespace.__dict__[key] = val[0]
      except Exception:
        Log.warn("heronrc: unable to clobber arguments (%s,%s ) ", namespace, args)
        Log.debug(traceback.format_exc())
    return namespace, args

def main():
  parser = HeronRCArgumentParser(
      prog='heron',
      epilog=help_epilog,
      formatter_class=config.SubcommandHelpFormatter,
      fromfile_prefix_chars='@',
      add_help=False,
      rcfile="./.heronrc")
  parser.add_subparsers(
      title="Available commands",
      metavar='<command> <options>')

  args, unknown_args = parser.parse_known_args()
  Log.info("parse results args: %s  unknown: %s ", args, unknown_args)


if __name__ == "__main__":
  sys.exit(main())
