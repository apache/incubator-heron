# Copyright 2016 Twitter. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
''' rest.py '''
import requests

ROUTE_SIGNATURES = {
    'activate': (requests.post, '/api/v1/topologies', ['name', 'cluster', 'role', 'env', 'user']),
    'deactivate': (requests.post, '/api/v1/topologies', ['name', 'cluster', 'role', 'env', 'user']),
    'kill': (requests.delete, '/api/v1/topologies', ['name', 'cluster', 'role', 'env', 'user']),
    'submit': (requests.post, '/api/v1/topologies', ['name', 'cluster', 'role', 'env', 'user']),
}
