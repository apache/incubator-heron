README.md for heronrc

features :


1. supports the following patterns

	1. activate:* --config-path cmd-activate-*                   #command specific
	2. :devcluster/ads/PROD --config-path cmd-invalid     #ignored
	3. submit:devcluster/ads/PROD --config-path cmd-submit-role   #role and command specific
	4. activate:devcluster/ads/PROD --config-path cmd-activate-role   #role and command specific
	5. --config-path hello   #ignored
	6. *:* --config-path hello-global   #global


2. handles removal of comments

3. Precedence - 
	1. command line
	2. command / cluster-env-role specific arguments (patterns #3 and #4 above)
	3. command specific argument (pattern #1 above)
	4. global arguments (pattern #6 above)



Assumptions:
command and role/cluster/env are positional arguments #1 and #2 to all heron cli commands


To run/Test : (make sure the __init__.py files are in all places)

cp.heronrc.sample $HOME/.heronrc
python  -m heron.cli.src.python.argparser activate devcluster/ads/PROD 12313 --config-path "this-is-it"


