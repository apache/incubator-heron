#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

'''
statemanagerfactory.py
Factory function that instantiates and connects to the requested
state managers based on a conf file.
Returns these state managers.
'''

import os


from heron.statemgrs.src.python.filestatemanager import FileStateManager
from heron.statemgrs.src.python.log import Log as LOG
from heron.statemgrs.src.python.zkstatemanager import ZkStateManager

def get_all_state_managers(conf):
  """
  @param conf - An instance of Config class
  Reads the config for requested state managers.
  Instantiates them, start and then return them.
  """
  state_managers = []
  try:
    state_managers.extend(get_all_zk_state_managers(conf))
    state_managers.extend(get_all_file_state_managers(conf))
    return state_managers
  except Exception as ex:
    LOG.error("Exception while getting state_managers.")
    raise ex

def get_all_zk_state_managers(conf):
  """
  Creates all the zookeeper state_managers and returns
  them in a list
  """
  state_managers = []
  state_locations = conf.get_state_locations_of_type("zookeeper")
  for location in state_locations:
    name = location['name']
    hostport = location['hostport']
    hostportlist = []
    for hostportpair in hostport.split(','):
      host = None
      port = None
      if ':' in hostport:
        hostandport = hostportpair.split(':')
        if len(hostandport) == 2:
          host = hostandport[0]
          port = int(hostandport[1])
      if not host or not port:
        raise Exception(f"Hostport for {name} must be of the format 'host:port'.")
      hostportlist.append((host, port))
    tunnelhost = location['tunnelhost']
    rootpath = location['rootpath']
    LOG.info("Connecting to zk hostports: " + str(hostportlist) + " rootpath: " + rootpath)
    state_manager = ZkStateManager(name, hostportlist, rootpath, tunnelhost)
    state_managers.append(state_manager)

  return state_managers

def get_all_file_state_managers(conf):
  """
  Returns all the file state_managers.
  """
  state_managers = []
  state_locations = conf.get_state_locations_of_type("file")
  for location in state_locations:
    name = location['name']
    rootpath = os.path.expanduser(location['rootpath'])
    LOG.info("Connecting to file state with rootpath: " + rootpath)
    state_manager = FileStateManager(name, rootpath)
    state_managers.append(state_manager)

  return state_managers
