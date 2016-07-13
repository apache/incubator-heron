README.md for heronrc

supports the following patterns
handles comments



submit:all --config-path world                   #command specific
all: --config-path noenvstar                     #global
all:* --config-path noenvstar                    #global    
:devcluster/ads/PROD --config-path nocmdstar     #role specific
*:devcluster/ads/PROD --config-path nocmdstar    #role specific
submit:devcluster/ads/PROD --config-path nocmd   #role and command specific
--config-path hello   #global
*:* --config-path hello   #global

requires 
--command and --role parameters as mandatory inputs as they are the keys


run
# python test.py
