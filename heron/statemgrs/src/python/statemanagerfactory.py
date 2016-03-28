import os

"""
Factory function that instantiates and connects to the requested
state managers based on a conf file.
Returns these state managers.
"""

from heron.statemgrs.src.python.config import Config
from heron.statemgrs.src.python.filestatemanager import FileStateManager
from heron.statemgrs.src.python.log import Log as LOG
from heron.statemgrs.src.python.zkstatemanager import ZkStateManager

def get_all_state_managers(conf):
  """
  Reads the config file for requested state managers.
  Instantiates them, start and then return them.
  """
  state_managers = []
  state_managers.extend(get_all_zk_state_managers(conf))
  state_managers.extend(get_all_file_state_managers(conf))
  return state_managers

def get_all_zk_state_managers(conf):
  """
  Connects to all the zookeeper state_managers and returns
  the connected state_managers instances.
  """
  state_managers = []
  state_locations = Config(conf).get_state_locations_of_type("zookeeper")
  for location in state_locations:
    name = location['name']
    host = location['host']
    port = location['port']
    tunnelhost = location['tunnelhost']
    rootpath = location['rootpath']
    LOG.info("Connecting to zk hostport: " + host + ":" + str(port) + " rootpath: " + rootpath)
    state_manager = ZkStateManager(name, host, port, rootpath, tunnelhost)
    try:
      state_manager.start()
    except Exception as e:
      LOG.error("Exception while connecting to state_manager.")
      traceback.print_exc()
    state_managers.append(state_manager)

  return state_managers

def get_all_file_state_managers(conf):
  """
  Returns all the file state_managers.
  """
  state_managers = []
  state_locations = Config(conf).get_state_locations_of_type("file")
  for location in state_locations:
    name = location['name']
    rootpath = os.path.expanduser(location['rootpath'])
    LOG.info("Connecting to file state with rootpath: " + rootpath)
    state_manager = FileStateManager(name, rootpath)
    try:
      state_manager.start()
    except Exception as e:
      LOG.error("Exception while connecting to state_manager.")
      traceback.print_exc()
    state_managers.append(state_manager)

  return state_managers
