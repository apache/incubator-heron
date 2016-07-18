#!/usr/bin/env python2.7

import argparse
import os
import sys
import shlex
import re
import collections
import json

################################################################################
# Run the command
################################################################################

HERON_RC_FILE = "~/.heronrc"
HERON_RC=os.path.expanduser(HERON_RC_FILE)
HERON_RC_SPL='@'+ HERON_RC
p = re.compile('(^[^\:]*):([^\s]*) (.*)')
filters       = ['^@']
expressions   = [re.compile(x) for x in filters]


class HeronRCArgumentParser(argparse.ArgumentParser):

 	def __init__(self, *args, **kwargs):
		self.cmdmap=collections.defaultdict(dict)
		self.cmdmap['*']['*'] = ''

		self.initializeFromRC()
		super(HeronRCArgumentParser, self).__init__(*args, **kwargs)

	def remove_comments(self,string):
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

	def initializeFromRC(self):
		if os.path.exists(HERON_RC):

			with open(HERON_RC) as f:
				for line in f:
					m = p.match(line)
					value = ''
					command='*'
					env = '*'
					if (m != None):
						value = self.remove_comments(m.group(3).rstrip(os.linesep))
						command = ( m.group(1),'') [ m.group(1) == None  ]
						env = ( m.group(2), '') [ m.group(2) == None ]
					else:
						value = self.remove_comments(line.rstrip(os.linesep))

					args_list = insert_bool_values(value.split())  # make sure that all the single args have a boolean value associated so that we can load the args to a key value structure
					args_list_string = ' '.join (args_list)

					if ( command in self.cmdmap and env in self.cmdmap[command]):
						self.cmdmap[command][env] =  self.cmdmap[command][env] 	+ ' ' + args_list_string	
					else:
						self.cmdmap[command][env] =  args_list_string				

		else:
			print "WARN: %s is not an existing file" % HERON_RC


    # for each command / cluster-role-env combination, get the commands from heronrc
    #     remove any duplicates that have already been supplied already  and present in the command-dictionary     
	def get_args_for_command_role (self , command, role, commanddict):
		args_for_command_role=''
		new_arg_strings = []

		if ( command in self.cmdmap and role in self.cmdmap[command]):
			args_for_command_role = (self.cmdmap[command][role],args_for_command_role) [self.cmdmap[command][role] == None or self.cmdmap[command][role] == '']

		newcommanddict=dict(zip(*[iter(args_for_command_role.split())]*2))
		for key,value in newcommanddict.items():
			if ( not key in commanddict ):
				commanddict[key] = value
				new_arg_strings.append (key)
				new_arg_strings.append(value)

		return new_arg_strings

	# this is invoked when the parser.parse_args is called
	#  apply the commands in the following precedence order
	#  use the defaults in the command line

	def _read_args_from_files(self,arg_strings):
		new_arg_strings = []
		commanddict=dict(zip(*[iter(arg_strings)]*2))
		for arg_string in arg_strings[2:]:
    		if not arg_string or not arg_string.startswith('@'):
    			new_arg_strings.append(arg_string)
		commanddict=dict(zip(*[iter(arg_strings[2:])]*2))	
		#print sys.argv[1] , sys.argv[2], commanddict
		command = self.get_default('subcommand')
		role = sys.argv[2]
		new_arg_strings.extend(self.get_args_for_command_role(command, role, commanddict))
		new_arg_strings.extend(self.get_args_for_command_role(command, '*',commanddict))
		new_arg_strings.extend(self.get_args_for_command_role('*', '*',commanddict))
		new_arg_strings.extend(arg_strings)
		print new_arg_strings
		return new_arg_strings


## the below two functions should be moved to util

def insert_bool(param, command_args):
  index = 0 
  found = False
  for lelem in command_args:
    if lelem == '--' and not found:
      break
    if lelem == param:
      found = True
      break
    index = index + 1   

  if found:
    command_args.insert(index + 1, 'True')
  return command_args 

def insert_bool_values(command_line_args):
  args1 = insert_bool('--verbose', command_line_args)
  args2 = insert_bool('--deploy-deactivated', args1)
  args3 = insert_bool('--trace-execution', args2)
  return args3

#test stub
def main():
	parser = HeronRCArgumentParser(description="test parser",fromfile_prefix_chars='@')
	parser.set_defaults(subcommand='submit')
	args = parser.parse_args(['submit', 'devcluster/ads/PROD', '123123'])


if __name__ == "__main__":
	sys.exit(main())