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

''' rest.py '''
import requests

ROUTE_SIGNATURES = {
    'activate': (requests.post, '/api/v1/topologies/%s/%s/%s/%s/activate', []),
    'deactivate': (requests.post, '/api/v1/topologies/%s/%s/%s/%s/deactivate', []),
    'kill': (requests.delete, '/api/v1/topologies/%s/%s/%s/%s', []),
    'restart': (requests.post, '/api/v1/topologies/%s/%s/%s/%s/restart', []),
    'submit': (requests.post, '/api/v1/topologies', ['name', 'cluster', 'role', 'env', 'user']),
    'update': (requests.post, '/api/v1/topologies/%s/%s/%s/%s/update', []),
    'version': (requests.get, '/api/v1/version', []),
}
