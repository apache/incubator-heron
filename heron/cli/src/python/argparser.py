#!/usr/bin/env python2.7

import argparse
import os
import sys
import shlex
import re
import collections
import json
import heron.cli.src.python.activate as activate
import heron.cli.src.python.args as args
import traceback

################################################################################
# Run the command
################################################################################

HERON_RC_FILE = "~/.heronrc"
HERON_RC=os.path.expanduser(HERON_RC_FILE)
HERON_RC_SPL='@'+ HERON_RC
p = re.compile('(^[^\:]*):([^\s]*) (.*)')
filters       = ['^@']
expressions   = [re.compile(x) for x in filters]

help_epilog = '''Getting more help: 
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''



class SubcommandHelpFormatter(argparse.RawDescriptionHelpFormatter):
  def _format_action(self, action):
    parts = super(argparse.RawDescriptionHelpFormatter, self)._format_action(action)
    if action.nargs == argparse.PARSER:
      parts = "\n".join(parts.split("\n")[1:])
    return parts

class HeronRCArgumentParser(argparse.ArgumentParser):
	cmdmap=collections.defaultdict(dict)
 	def __init__(self, *args, **kwargs):
		HeronRCArgumentParser.initializeFromRC()
		super(HeronRCArgumentParser, self).__init__(*args, **kwargs)
	
	@classmethod
	def remove_comments(hrc,string):
		pattern = r"(\#.*$)"
		# first group captures quoted strings (double or single)
		# second group captures comments (//single-line or /* multi-line */)
		regex = re.compile(pattern, re.MULTILINE|re.DOTALL)
		def _replacer(match):
		    # if the 2nd group (capturing comments) is not None,
		    # it means we have captured a non-quoted (real) comment string.
		    if match.group(1) is not None:
		        return "" # so we will return empty to remove the comment
		    else: # otherwise, we will return the 1st group
		        return match.group(1) # captured quoted-string
		return regex.sub(_replacer, string)

	# initialize the command map from heron rc file in the parser, that can be later used for command substitution during parse_args phase
	# patterns
	# cmdmap [cmd][environment] = 'argument list'
	# cmdmap ['submit']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	# cmdmap ['*']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	# cmdmap ['*']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	@classmethod
	def initializeFromRC(hrc):
		if  len(hrc.cmdmap) > 0:
			return 
		if os.path.exists(HERON_RC):

			with open(HERON_RC) as f:
				hrc.cmdmap['*']['*'] = ''
				for line in f:
					m = p.match(line)
					value = ''
					command=''
					env = ''
					if (m != None):
						value = hrc.remove_comments(m.group(3).rstrip(os.linesep))
						command = ( m.group(1),'') [ m.group(1) == None or m.group(1) == '' ]
						env = ( m.group(2), '') [ m.group(2) == None or m.group(2) == '']
					else:
						value = hrc.remove_comments(line.rstrip(os.linesep))

					args_list = args.insert_bool_values(value.split())  # make sure that all the single args have a boolean value associated so that we can load the args to a key value structure
					args_list_string = ' '.join (args_list)
					if command == None or command == '' :
						continue
					if ( command in hrc.cmdmap and env in hrc.cmdmap[command]):
						hrc.cmdmap[command][env] =  hrc.cmdmap[command][env] 	+ ' ' + args_list_string	
					else:
						hrc.cmdmap[command][env] =  args_list_string
			#print "cmdmap", hrc.cmdmap	
		else:
			print "WARN: %s is not an existing file" % HERON_RC


    # for each command / cluster-role-env combination, get the commands from heronrc
    #     remove any duplicates that have already been supplied already  and present in the command-dictionary     
	@classmethod
	def get_args_for_command_role (hrc, command, role):
		args_for_command_role=''

		if ( command in hrc.cmdmap and role in hrc.cmdmap[command]):
			args_for_command_role = (hrc.cmdmap[command][role],args_for_command_role) [hrc.cmdmap[command][role] == None or hrc.cmdmap[command][role] == '']
		return args_for_command_role.split()

	# this is invoked when the parser.parse_args is called
	#  apply the commands in the following precedence order
	#  use the defaults in the command line

	def _read_args_from_files(self,arg_strings):
		new_arg_strings = []
		command = sys.argv[1]
		role = sys.argv[2]
		new_arg_strings.extend(self.get_args_for_command_role('*', '*'))
		new_arg_strings.extend(self.get_args_for_command_role(command, '*'))
		new_arg_strings.extend(self.get_args_for_command_role(command, role))
		new_arg_strings.extend(arg_strings)
		return arg_strings

#test stub
def main():
	#parser = HeronRCArgumentParser(description="test parser",fromfile_prefix_chars='@')
	  #parser = argparse.ArgumentParser(
	parser = HeronRCArgumentParser(
		prog = 'heron',
		epilog = help_epilog,
		formatter_class=SubcommandHelpFormatter,
		fromfile_prefix_chars='@',
		add_help = False)
	subparsers = parser.add_subparsers(
		title = "Available commands", 
		metavar = '<command> <options>')

	activate.create_parser(subparsers)
	args, unknown_args = parser.parse_known_args()
	print args, unknown_args


if __name__ == "__main__":
	sys.exit(main())