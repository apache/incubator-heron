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
		#print rcfile, effective_rc
		if os.path.exists(effective_rc):

			with open(effective_rc) as f:
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

					args_list = utils.insert_bool_values(value.split())  # make sure that all the single args have a boolean value associated so that we can load the args to a key value structure
					args_list_string = ' '.join (args_list)
					if command == None or command == '' :
						continue
					if ( command in hrc.cmdmap and env in hrc.cmdmap[command]):
						hrc.cmdmap[command][env] =  hrc.cmdmap[command][env] 	+ ' ' + args_list_string	
					else:
						hrc.cmdmap[command][env] =  args_list_string
			#print "cmdmap", hrc.cmdmap	
		else:
			print "WARN: %s is not an existing file" % effective_rc


    # for each command / cluster-role-env combination, get the commands from heronrc
    #     remove any duplicates that have already been supplied already  and present in the command-dictionary     
	@classmethod
	def get_args_for_command_role (hrc, command, role):
		args_for_command_role=''

		if ( command in hrc.cmdmap and role in hrc.cmdmap[command]):
			args_for_command_role = (hrc.cmdmap[command][role],args_for_command_role) [hrc.cmdmap[command][role] == None or hrc.cmdmap[command][role] == '']
		#print "get_args_for_command_role", command, role, args_for_command_role
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
		#command = arg_strings[1]
		#role = arg_strings[2]

		new_arg_strings.extend(self.get_args_for_command_role('*', '*'))
		new_arg_strings.extend(self.get_args_for_command_role(command, '*'))
		new_arg_strings.extend(self.get_args_for_command_role(command, role))
		#new_arg_strings.extend(arg_strings)
		arg_strings.extend(new_arg_strings)
		#print 
		#print sys.argv , command , role, new_arg_strings
		return arg_strings

	def _xparse_known_args(self, args=None, namespace=None):
		print "entering in parse_known_args", namespace,args
		if args is None:
		    # args default to the system args
		    args = sys.argv[1:]
		else:
		    # make sure that args are mutable
		    args = list(args)

		# default Namespace built from parser defaults
		if namespace is None:
		    namespace = argparse.Namespace()
		# add any action defaults that aren't present
		for action in self._actions:
		    if action.dest is not argparse.SUPPRESS:
		        if not hasattr(namespace, action.dest):
		            if action.default is not argparse.SUPPRESS:
		                setattr(namespace, action.dest, action.default)

		# add any parser defaults that aren't present
		for dest in self._defaults:
		    if not hasattr(namespace, dest):
		        setattr(namespace, dest, self._defaults[dest])

		# parse the arguments and exit if there are any errors
		try:
			print "3 parse_known_args--" , namespace,args
			namespace, args = self._parse_known_args(args, namespace)

			if hasattr(namespace, argparse._UNRECOGNIZED_ARGS_ATTR):
			    args.extend(getattr(namespace, argparse._UNRECOGNIZED_ARGS_ATTR))
			    delattr(namespace, argparse._UNRECOGNIZED_ARGS_ATTR)
			print "4 parse_known_args--" , namespace,args
			return namespace, args
		except argparse.ArgumentError:
		    err = sys.exc_info()[1]
		    self.error(str(err))

	def _parse_known_args(self, arg_strings, namespace):
		#print "ENTERING in _parse_known_args", arg_strings, namespace
		namespace, args = super(HeronRCArgumentParser, self)._parse_known_args(arg_strings, namespace)
		#print "EXITING _parse_known_args--" , arg_strings, namespace
		return namespace, args
	
	def parse_known_args(self, args=None, namespace=None):
		#print "ENTERING in parse_known_args", args
		namespace, args = super(HeronRCArgumentParser, self).parse_known_args(args, namespace)
		#print "EXITING parse_known_args--" , namespace

		if self.prog != 'heron':
			
			try:
				print "in HeronRCArgumentParser._parse_known_args", self, namespace, args
				raise ValueError('A very specific bad thing happened')
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