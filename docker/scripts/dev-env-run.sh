#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This is a script to start a new session of the docker container
# created with the dev-env-create.sh script.
# Usage:
#   sh docker/scripts/dev-env-run.sh CONTAINER_NAME
#
# In case you forgot the container name, you can use this command
# to list all the existing containers (name is the last column).
#   docker container ls -a

set -o nounset
set -o errexit

case $# in
  0)
    echo "Missing argument."
    echo "Usage: $0 <container_name>"
    exit 1
    ;;
esac

CONTAINER_NAME=$1

docker container start $CONTAINER_NAME
docker exec -it $CONTAINER_NAME bash