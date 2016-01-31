################################################################################
# Get the topology jars - TODO, make the jars independent version free
################################################################################
def get_topology_jars():
  jars = [
      'lib/protobuf-java-2.5.0.jar',
      'lib/log4j-over-slf4j-1.7.7.jar',
      'lib/slf4j-api-1.7.7.jar',
      'lib/slf4j-jdk14-1.7.7.jar'
  ]
  return jars

################################################################################
# Get the scheduler jars
################################################################################
def get_scheduler_jars():
  jars = [
      'lib/heron-scheduler.jar'
  ]
  return jars
