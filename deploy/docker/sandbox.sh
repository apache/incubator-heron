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
set -o errexit

bold=$(tput bold)
normal=$(tput sgr0)
c_name="heron-sandbox"

check_docker_install() {
  if ! [[ $(which docker) && $(docker --version) ]]; then
    echo "Docker is not currently installed on your system"
    exit 1
  fi
}

sandbox_start() {
  docker run -d -p 8889:8889 -p 9000:9000 --name=$c_name $1 > /dev/null
  echo "${bold}The Heron Sandbox is up and running ${normal}"
}

sandbox_ps() {
  docker ps -f name=$c_name
}

sandbox_clogs() {
  docker logs $c_name
}

sandbox_shell() {
  echo "${bold}Starting the Heron Sandbox shell${normal}"
  docker exec -it $c_name /bin/bash
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to create a shell session for the container '$c_name'"
    exit 1
  fi
  echo "${bold}Terminating the Heron Sandbox shell${normal}"
}

sandbox_stop() {
  docker stop $c_name > /dev/null 2>&1
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to stop the container '$c_name'"
    exit 1
  fi
  docker rm -f -v $c_name > /dev/null 2>&1
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to remove the container '$c_name'"
    exit 1
  fi
  echo "${bold}The Heron Sandbox has been successfully shut down${normal}"
}

check_docker_install

case $1 in
  start)
    DOCKER_IMAGE=${2:-apache/heron:latest}
    sandbox_start $DOCKER_IMAGE
    ;;
  ps)
    sandbox_ps
    ;;
  clogs)
    sandbox_clogs
    ;;
  stop)
    sandbox_stop
    ;;
  shell)
    sandbox_shell
    ;;
  help|*)
    echo "  "
    echo "${bold}The Heron Sandbox CLI tool${normal}"
    echo "=========================="
    echo "  "
    echo "The Heron Sandbox is a version of Heron that runs inside of a single Docker container."
    echo "Example topologies are included inside the container as well, making the Heron Sandbox"
    echo "an ideal tool for initial experimentation with Heron."
    echo "  "
    echo "${bold}Starting and shutting down the Heron Sandbox${normal}"
    echo "  "
    echo "  ${bold}Start${normal} the Heron Sandbox:"
    echo "    $0 start"
    echo "  "

    echo "  ${bold}Shut down${normal} the Heron Sandbox:"
    echo "    $0 stop"
    echo "  "

    echo "  ${bold}Check${normal} if the Heron Sandbox is running:"
    echo "    $0 ps"
    echo "  "

    echo "${bold}Example topologies included with the Heron Sandbox${normal}"
    echo "  "
    echo "  Heron ${bold}Java API${normal} example topologies are available in:"
    echo "    /heron/examples/heron-api-examples.jar"
    echo "  "

    echo "  Heron ${bold}Java Streamlet API${normal} example topologies are available in:"
    echo "    /heron/examples/heron-streamlet-examples.jar"
    echo "  "

    echo "  Heron ${bold}Java ECO${normal} example topologies are available in:"
    echo "    /heron/examples/heron-eco-examples.jar"
    echo "  "

    echo "${bold}Experimenting with the examples in the Heron Sandbox${normal}"
    echo "  "
    echo "  First, ${bold}open a shell session${normal} to play with example topologies:"
    echo "    $0 shell"
    echo "  "
    echo "      ${bold}NOTE${normal}: Make sure Docker is running on your machine first. See instructions"
    echo "            here: https://docs.docker.com/machine/get-started"
    echo "  "

    echo "  Then you can ${bold}submit${normal} a topology via the shell:"
    echo "    heron submit sandbox /heron/examples/heron-api-examples.jar org.apache.heron.examples.api.ExclamationTopology exclamation"
    echo "  "

    echo "  You can ${bold}deactivate${normal} the topology via the shell as well:"
    echo "    heron deactivate sandbox exclamation"
    echo "  "

    echo "  To ${bold}activate${normal} a deactivated topology:"
    echo "    heron activate sandbox exclamation"
    echo "  "

    echo "  Finally, you can ${bold}kill${normal} a topology and remove it from the cluster:"
    echo "    heron kill sandbox exclamation"
    echo "  "

    exit 1
    ;;
esac
