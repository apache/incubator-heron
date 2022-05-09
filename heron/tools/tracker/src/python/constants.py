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

""" constants.py """

import heron.tools.common.src.python.utils.config as common_config

# This file contains all the constants used
# across the tracker service.

# Version Information

try:
  API_VERSION = common_config.get_version_number()
except:
  API_VERSION = "0.1.0"


# Handler Constants

# Parameter Names

PARAM_CLUSTER = "cluster"
PARAM_COMPONENT = "component"
PARAM_CONTAINER = "container"
PARAM_ENDTIME = "endtime"
PARAM_ENVIRON = "environ"
PARAM_INSTANCE = "instance"
PARAM_INTERVAL = "interval"
PARAM_LENGTH = "length"
PARAM_METRICNAME = "metricname"
PARAM_OFFSET = "offset"
PARAM_PATH = "path"
PARAM_QUERY = "query"
PARAM_STARTTIME = "starttime"
PARAM_TOPOLOGY = "topology"
PARAM_ROLE = "role"

# These are the keys in the JSON response
# formed by the handlers.

RESPONSE_KEY_EXECUTION_TIME = "executiontime"
RESPONSE_KEY_MESSAGE = "message"
RESPONSE_KEY_RESULT = "result"
RESPONSE_KEY_STATUS = "status"
RESPONSE_KEY_VERSION = "tracker_version"

# These are the values of the status
# in the JSON repsonse.

RESPONSE_STATUS_FAILURE = "failure"
RESPONSE_STATUS_SUCCESS = "success"

# Timeout for HTTP requests.

HTTP_TIMEOUT = 5 #seconds

# default parameter - port for the tracker to listen on
DEFAULT_PORT = 8888

# default config file to read
DEFAULT_CONFIG_FILE = "heron_tracker.yaml"

# default paramater - type of state manaager
DEFAULT_STATE_MANAGER_TYPE = "file"

# default parameter - name to be used for the state manager
DEFAULT_STATE_MANAGER_NAME = "local"

# default parameter - where all the states are stored
DEFAULT_STATE_MANAGER_ROOTPATH = "~/.herondata/repository/state/local"

# default parameter - if ssh tunneling needs to be established to connect to it
DEFAULT_STATE_MANAGER_TUNNELHOST = "127.0.0.1"

# default parameter - only used to connect to zk, must be of the form host:port
DEFAULT_STATE_MANAGER_HOSTPORT = "127.0.0.1:2181"
