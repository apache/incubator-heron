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
		# patterns
		# cmdmap [cmd][environment] = 'argument list'
		# cmdmap ['submit']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
		# cmdmap ['submit']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
		# cmdmap ['*']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
		# cmdmap ['*']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'

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
	def initializeFromRC(self):
		if os.path.exists(HERON_RC):
			#print "file exists\n"

			with open(HERON_RC) as f:
				for line in f:
					#print 'line' + line
					m = p.match(line)
					value = ''
					command='*'
					env = '*'
					if (m != None):
						value = self.remove_comments(m.group(3).rstrip(os.linesep))
						command = ( m.group(1),'*') [ m.group(1) == None or m.group(1) == 'all' or  m.group(1) == '']
						env = ( m.group(2), '*') [ m.group(2) == None or m.group(2) == 'all' or  m.group(2) == '']
						#print command , env , value
					else:
						#print 'no match' + line
						value = self.remove_comments(line.rstrip(os.linesep))

					if ( command in self.cmdmap and env in self.cmdmap[command]):
						self.cmdmap[command][env] =  self.cmdmap[command][env] 	+ ' ' + value	
					else:
						self.cmdmap[command][env] =  value				

			jsonarray = json.dumps(self.cmdmap)
			print jsonarray
		else:
			print "WARN: %s is not an existing file" % HERON_RC

	def get_args_for_command_role (self , command, role):
		args_for_command_role=''
		if ( command in self.cmdmap and role in self.cmdmap[command]):
			args_for_command_role = (self.cmdmap[command][role],args_for_command_role) [self.cmdmap[command][role] == None or self.cmdmap[command][role] == '']
		print args_for_command_role , command , role
		return args_for_command_role

	def _read_args_from_files(self,arg_strings):
		new_arg_strings = []
		commanddict=dict(zip(*[iter(arg_strings)]*2))
		for arg_string in arg_strings:
			if not arg_string or not arg_string.startswith('@'):
				new_arg_strings.append(arg_string)
		
		commanddict=dict(zip(*[iter(arg_strings)]*2))	
		command = commanddict['--command']
		role = commanddict ['--role']
		new_arg_strings.append(self.get_args_for_command_role('*', '*'))
		new_arg_strings.append(self.get_args_for_command_role(command, '*'))
		new_arg_strings.append(self.get_args_for_command_role('*', role))
		new_arg_strings.append(self.get_args_for_command_role(command, role))
		new_arg_strings.extend(arg_strings)

		return new_arg_strings




def main():
	parser = HeronRCArgumentParser(description="test parser",fromfile_prefix_chars='@')
	args = parser.parse_args(['--role', 'devcluster/ads/PROD' , '--command', 'submit' ])


if __name__ == "__main__":
	sys.exit(main())