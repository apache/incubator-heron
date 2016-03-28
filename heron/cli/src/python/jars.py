import os, fnmatch

import heron.cli.src.python.utils as utils

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
       os.path.join(utils.get_heron_lib_dir(), "scheduler", "*")
  ]
  return jars

################################################################################
# Get the uploader jars
################################################################################
def uploader_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "uploader", "*")
  ]
  return jars

################################################################################
# Get the statemgr jars
################################################################################
def statemgr_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "statemgr", "*")
  ]
  return jars

################################################################################
# Get the packing algorithm jars
################################################################################
def packing_jars():
  jars = [
      os.path.join(utils.get_heron_lib_dir(), "packing", "*")
  ]
  return jars
