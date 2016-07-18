#!/usr/bin/env python2.7

import argparse
import os
import sys
import shlex
import re

################################################################################
# Run the command
################################################################################
HERON_RC_FILE = "~/.heronrc"
HERON_RC=os.path.expanduser(HERON_RC_FILE)
HERON_RC_SPL='@'+HERON_RC
p = re.compile('(^[^\:]*):([^\s]*) (.*)')

cmdmap=collections.defaultdict(dict)
# patterns
# cmdmap [cmd][environment] = 'argument list'
# cmdmap ['submit']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
# cmdmap ['submit']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
# cmdmap ['*']['devcluster/ads/PROD'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'
# cmdmap ['*']['*'] = '--config-property "heron.class.launcher=com.twitter.heron.scheduler.aurora.AuroraLauncher"'

cmdmap={'all':{'all':''}}

class HeronRCArgumentParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(HeronRCArgumentParser, self).__init__(*args, **kwargs)

    def initializeFromRC():
        if os.path.exists(HERON_RC):
   			print "file exists\n"

   			with open(HERON_RC) as f:
   				for line in f:
   					print 'line' + line
   					m = p.match(line)
   					if (m != None):
   						print m
   						command = ('*', m.group(1)) [ m.group(1) != None ]
   						env = ('*', m.group(2)) [ m.group(2) != None ]
   						cmdmap[command][env] = m.group(3)
   						#cmdmap=dict([line.split(":")])
   					else:
   			    		print 'no match' + line
   			    		cmdmap['*']['*'] = line
   						#cmdmap=dict([line.split(":")])
   			print cmdmap
   		else:
   			print "WARN: %s is not an existing file" % HERON_RC)

    def _read_args_from_files(self,arg_strings):
    	new_arg_strings = []
    	for arg_string in arg_strings:
    		if not arg_string or not arg_string.startswith('@'):
    			new_arg_strings.append(arg_string)
    		else:
    			with open(arg_string[1:]) as args_file:
    				arg_strings = shlex.split(args_file.read())
    				new_arg_strings.extend(arg_strings)
		return new_arg_strings

    def convert_arg_line_to_args(self, line):
        for arg in line.split():
        	print arg
        	if not arg.strip():
        		continue
    		if arg[0] == '#':
    			break
			yield arg
		



def main():
   if os.path.exists(HERON_RC):
   	print "file exists\n"

   	with open(HERON_RC) as f:
   		for line in f:
   			print 'line' + line
   			m = p.match(line)
   			if (m != None):
   				print m
   				cmdmap[m.group(1)+':' + m.group(2)] = m.group(3)
   				#cmdmap=dict([line.split(":")])
   			else:
   			    print 'no match' + line
   			    cmdmap['all'] = line
   			#cmdmap=dict([line.split(":")])
   		print cmdmap
   else:
   	raise Exception("%s is not an existing file" % HERON_RC)
   parser = HeronRCArgumentParser(description="test parser",fromfile_prefix_chars='@')
   args = parser.parse_args([HERON_RC_SPL])


if __name__ == "__main__":
	sys.exit(main())