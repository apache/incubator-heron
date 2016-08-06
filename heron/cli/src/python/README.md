#README.md for heronrc


##Overview:
Heron RC is a dictionary of commonly used configurations that doesnt have to be repeatedly supplied via command line
Developers and Operators can configure standard options to heron-cli or heron-tracker via heronrc  and these will be looked up at run time by the client and attached to existing args supplied via command line.

As is the case with any dictionary , there are a set of attributes that act as  keys, and there are values - which is the argument list.  

the key attributes are:  
	1. client application ex: heron (for heron-cli) or heron-tracker   
	2. sub command : ex. activate , kill  
	3. cluster/env/role : devcluster/ads/PROD  


HeronRC supports an implicit hierarchy so that argument collisions can be handled. 
Heron RC also discourages the use of positional arguments , and only recommends the use of key value pairs.

##Hierarchy:
	*client 
	   |___ *subcommand
	   			|_______*cluster/env/role

Heron RC supports wild card subsititutions in the above hierarchy. Please note that the supported wild card is just '*'. we dont support regex (not yet:)


##Features :

1. supports the following patterns

	* heron:submit:devcluster/ads/PROD --config-path cmd-submit-role   #role and command specific
	* heron:activate:devcluster/ads/PROD --config-path cmd-activate-role   #role and command specific
	* heron:activate:* --config-path cmd-activate-*                   #command specific
	*  *:*:* --config-path all-global   #global
	*  heron:*:* --config-path cmd-global    #app global
	*  :devcluster/ads/PROD --config-path cmd-invalid     #ignored
	*  --config-path hello   #ignored

2. handles removal of comments and invalid args (see patterns #f and #g)

3. Precedence - 
	*  command line  (any argument supplied over command line takes precedence over heronrc)
	*  command / cluster-env-role specific arguments (patterns #a and #b above)
	*  command specific argument (pattern #c above)
	*  global arguments at app level and uber level (pattern #d, #e above)


4. support for positional arguments
   	* HeronRC parser doesnt support positional arguments at this time. However, if there is a need to configure positional arguments in heron rc, we can do so by configuring their optional equivalents. Let's consider the following example for submitting the example topology to the local cluster via heron-cli
   	   it would be done through the following command line : 
           *
             ```  
                heron submit local --verbose ~/.heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
             ```  
            the same can be configured in heron rc by the following entry:  
            ````  
heron:submit:local --verbose  --topology-file-name ~/.heron/examples/heron-examples.jar  --topology-class-name com.twitter.heron.examples.ExclamationTopology --topology-name ExclamationTopology
	    ````  
	    you can get the optional equivalents of a command through help : ex: heron submit help  
	        

##Assumptions:
command and role/cluster/env are positional arguments and are in #1 and #2 to all heron RC supported applications
the application has a designated identifier :
	for example : heron-cli  has heron



To run/Test : 
(make sure the __init__.py files are in all places and you have a sample heron rc file in the directory - ex: heron/cli/tests/python/)


``````
unit2 -v heron.cli.tests.python.heronparser_unittest
or 
python -m heron.cli.tests.python.heronparser_unittest
or 
python -m unittest2 heron.cli.tests.python.heronparser_unittest

``````
```````
test_parser_commandline (__main__.HeronParserTest) ... ok
test_parser_norcfile (__main__.HeronParserTest) ... ok
test_parser_rolecmdspecific (__main__.HeronParserTest) ... ok

----------------------------------------------------------------------
Ran 3 tests in 0.006s

OK

````````
to build:
`````
./docker/build-docker.sh ubuntu14.04 0.14.1 ../heron-release
`````

