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

''' cdefs.py '''
import os

from heron.tools.cli.src.python import cliconfig
from heron.tools.common.src.python.utils import config

################################################################################
def read_server_mode_cluster_definition(cluster, cl_args):
  '''
  Read the cluster definition for server mode
  :param cluster:
  :param cl_args:
  :param config_file:
  :return:
  '''

  client_confs = {}
  client_confs[cluster] = cliconfig.cluster_config(cluster)

  # now check if the service-url from command line is set, if so override it
  if cl_args.get('service_url', None):
    client_confs[cluster]['service_url'] = cl_args['service_url']

  # the return value of yaml.load can be None if conf_file is an empty file
  # or there is no service-url in command line, if needed.

  return client_confs

################################################################################
def check_direct_mode_cluster_definition(cluster, config_path):
  '''
  Check the cluster definition for direct mode
  :param cluster:
  :param config_path:
  :return:
  '''
  config_path = config.get_heron_cluster_conf_dir(cluster, config_path)
  if not os.path.isdir(config_path):
    return False
  return True
