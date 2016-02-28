import os, fnmatch

import heron.cli3.src.python.utils as utils

################################################################################
# Get the topology jars - TODO, make the jars independent version free
################################################################################
def pick(dirname, pattern):
  file_list = fnmatch.filter(os.listdir(dirname), pattern)
  return file_list[0] if file_list else None
    
################################################################################
# Get the topology jars - TODO, make the jars independent version free
################################################################################
def topology_jars():
  jars = [
      pick(utils.get_heron_lib_dir(), 'protobuf-java-*.jar'),
      pick(utils.get_heron_lib_dir(), 'log4j-over-slf4j-*.jar'),
      pick(utils.get_heron_lib_dir(), 'slf4j-api-*.jar'),
      pick(utils.get_heron_lib_dir(), 'slf4j-jdk*.jar')
  ]
  return jars

################################################################################
# Get the scheduler jars
################################################################################
def scheduler_jars():
  jars = [
      pick(utils.get_heron_lib_dir(), 'heron-scheduler*.jar'),
      pick(utils.get_heron_lib_dir(), 'heron-local-scheduler*.jar')
  ]
  return jars

################################################################################
# Get the uploader jars
################################################################################
def uploader_jars():
  jars = [
      pick(utils.get_heron_lib_dir(), 'heron-localfs-uploader*.jar')
  ]
  return jars

################################################################################
# Get the statemgr jars
################################################################################
def statemgr_jars():
  jars = [
      pick(utils.get_heron_lib_dir(), 'heron-localfs-statemgr*.jar'),
      pick(utils.get_heron_lib_dir(), 'heron-zookeeper-statemgr*.jar')
  ]
  return jars

