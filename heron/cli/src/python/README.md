README.md for heronrc

supports the following patterns
handles comments



submit:all --config-path world                   #command specific
*:* --config-path noenvstar                    #global    
*:devcluster/ads/PROD --config-path nocmdstar    #role specific
submit:devcluster/ads/PROD --config-path nocmd   #role and command specific

requires 
--command and --role parameters as mandatory inputs as they are the keys


run
# python test.py
