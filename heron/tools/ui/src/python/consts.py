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
''' consts.py '''
import heron.tools.common.src.python.utils.config as common_config

# default parameter - address for the web to ui to listen on
DEFAULT_ADDRESS = "0.0.0.0"

# default parameter - port for the web to ui to listen on
DEFAULT_PORT = 8889

# default parameter - url to connect to heron tracker
DEFAULT_TRACKER_URL = "http://localhost:8888"

VERSION = common_config.get_version_number(zipped_pex=True)
