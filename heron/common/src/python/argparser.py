#!/usr/bin/env python2.7

import argparse
import os
import sys
import shlex
import re
import collections
import json
#import heron.cli.src.python.activate as activate
import heron.common.src.python.utils as utils
import traceback
from heron.common.src.python.color import Log
    

################################################################################
# Run the command
################################################################################

HERON_RC_FILE = "~/.heronrc"
HERON_RC=os.path.expanduser(HERON_RC_FILE)
HERON_RC_SPL='@'+ HERON_RC
heron_command_pattern = re.compile('(^[^\:]*):([^\:]*):([^\s]*) (.*)')
filters       = ['^@']
expressions   = [re.compile(x) for x in filters]

help_epilog = '''Getting more help: 
  heron help <command> Prints help and options for <command>

For detailed documentation, go to http://heronstreaming.io'''



class HeronRCArgumentParser(argparse.ArgumentParser):
	cmdmap=collections.defaultdict(dict)
	

 	def __init__(self, *args, **kwargs):
 		rcfile = HeronRCArgumentParser.getAndRemoveKey(kwargs, "rcfile")
 		self.rccommand = HeronRCArgumentParser.getAndRemoveKey(kwargs, "rccommand")
 		self.rcclusterrole = HeronRCArgumentParser.getAndRemoveKey(kwargs, "rcclusterrole")
		HeronRCArgumentParser.initializeFromRC(rcfile)
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

	@classmethod
	def getAndRemoveKey(hrc, dict, key):
		val = None
		if key in dict:
			val =  dict[key]
			del(dict[key])
		return val
	# initialize the command map from heron rc file in the parser, that can be later used for command substitution during parse_args phase
	# patterns
	# cmdmap [cmd][environment] = 'argument list'
	# cmdmap ['submit']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	# cmdmap ['*']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	# cmdmap ['*']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
	@classmethod
	def initializeFromRC(hrc,rcfile):
		if  len(hrc.cmdmap) > 0:
			return 
		effective_rc  = (rcfile, HERON_RC) [ rcfile == None] 
		Log.warn('Effective RC file is %s' % (effective_rc))
		if os.path.exists(effective_rc):

			with open(effective_rc) as f:
				hrc.cmdmap['*']['*'] = collections.defaultdict(dict)
				hrc.cmdmap['*']['*']['*'] = ''
				for line in f:
					m = heron_command_pattern.match(line)
					value = ''
					command=''
					app = ''
					env = ''
					if (m != None):
						value = hrc.remove_comments(m.group(4).rstrip(os.linesep))
						app = ( m.group(1),'') [ m.group(1) == None or m.group(1) == '' ]
						command = ( m.group(2),'') [ m.group(2) == None or m.group(1) == '' ]
						env = ( m.group(3), '') [ m.group(3) == None or m.group(2) == '']
					else:
						continue
						#value = hrc.remove_comments(line.rstrip(os.linesep))

					args_list = utils.insert_bool_values(value.split())  # make sure that all the single args have a boolean value associated so that we can load the args to a key value structure
					args_list_string = ' '.join (args_list)
					if command == None or command == '' :
						continue
					if app == None or app == '':
						continue
					if env == None or env == '':
						continue
					#print app , command  , env, args_list_string, hrc.cmdmap
					if ( app not in hrc.cmdmap) :
						hrc.cmdmap[app] = collections.defaultdict(dict)

					if ( command in hrc.cmdmap[app] and env in hrc.cmdmap[app][command]):
						hrc.cmdmap[app][command][env] =  hrc.cmdmap[app][command][env] 	+ ' ' + args_list_string	
					else:
						hrc.cmdmap[app][command][env] =  args_list_string
			Log.debug( "RC cmdmap %s"% json.dumps(hrc.cmdmap) )
		else:
			Log.warn("WARN: %s is not an existing file" % effective_rc)


    # for each command / cluster-role-env combination, get the commands from heronrc
    #     remove any duplicates that have already been supplied already  and present in the command-dictionary     
	@classmethod
	def get_args_for_command_role (hrc, app,command, role):
		args_for_command_role=''

		if ( app in hrc.cmdmap and command in hrc.cmdmap[app] and role in hrc.cmdmap[app][command]):
			args_for_command_role = (hrc.cmdmap[app][command][role],args_for_command_role) [hrc.cmdmap[app][command][role] == None]
		return args_for_command_role.split()

	# this is invoked when the parser.parse_args is called
	#  apply the commands in the following precedence order
	#  use the defaults in the command line

	def _read_args_from_files(self,arg_strings):
		new_arg_strings = []
		command = self.rccommand
		if len(sys.argv) > 1:
			command = (sys.argv[1], self.rccommand) [self.rccommand != '']
		role = self.rcclusterrole
		if len(sys.argv)>2 :
			role = (sys.argv[2], self.rcclusterrole) [self.rcclusterrole != '']
		app = self.prog
		new_arg_strings.extend(self.get_args_for_command_role(app, command, role))
		new_arg_strings.extend(self.get_args_for_command_role(app, command, '*'))
		new_arg_strings.extend(self.get_args_for_command_role(app, '*', '*'))
		new_arg_strings.extend(self.get_args_for_command_role('*','*', '*'))
		#new_arg_strings.extend(arg_strings)
		arg_strings.extend(new_arg_strings)
		return arg_strings


	
	def parse_known_args(self, args=None, namespace=None):
		namespace, args = super(HeronRCArgumentParser, self).parse_known_args(args, namespace)

		if self.prog == 'heron':			
			try:
				for key in namespace.__dict__:
					val = namespace.__dict__[key]
					if val is not None and type(val) is list :
						namespace.__dict__[key] = val[0]
			except Exception as error:
				traceback.print_stack()
			return namespace,args	

			#logging.exception("Something awful happened!")
		return namespace, args
#test stub
def main():
	#parser = HeronRCArgumentParser(description="test parser",fromfile_prefix_chars='@')
	  #parser = argparse.ArgumentParser(
	parser = HeronRCArgumentParser(
		prog = 'heron',
		epilog = help_epilog,
		formatter_class=utils.SubcommandHelpFormatter,
		fromfile_prefix_chars='@',
		add_help = False,
		rcfile = "./.heronrc")
	subparsers = parser.add_subparsers(
		title = "Available commands", 
		metavar = '<command> <options>')

	args, unknown_args = parser.parse_known_args()
	print args, unknown_args


if __name__ == "__main__":
	sys.exit(main())