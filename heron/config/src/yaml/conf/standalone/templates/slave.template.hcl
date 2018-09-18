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

# Increase log verbosity
log_level = "DEBUG"

# Setup data dir
data_dir = "/tmp/slave"

# Enable the client
client {
    enabled = true
    servers = [<nomad_masters:master_port>]
    options = {
     "driver.raw_exec.enable" = "1"
  }
}

# Modify our port to avoid a collision with server1
ports {
    http = 5656
}
