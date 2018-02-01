#!/bin/bash
set -o errexit

bold=$(tput bold)
normal=$(tput sgr0)

check_docker_install() {
  if ! [[ $(which docker) && $(docker --version) ]]; then
    echo "Docker is not installed in system"
    exit 1
  fi
}

sandbox_start() {
  docker run -d -p 8889:8889 -p 9000:9000 --name=heron $1 > /dev/null
  echo "${bold}Heron sandbox is up and running ${normal}"
}

sandbox_ps() {
  docker ps -f name=heron
}

sandbox_clogs() {
  docker logs heron
}

sandbox_shell() {
  echo "${bold}Starting heron sandbox shell ${normal}"
  docker exec -it heron /bin/bash
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to create shell to the container 'heron'"
    exit 1
  fi
  echo "${bold}Terminating heron sandbox shell ${normal}"
}

sandbox_stop() {
  docker stop heron > /dev/null 2>&1
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to stop the container 'heron'"
    exit 1
  fi
  docker rm -f -v heron > /dev/null 2>&1
  EXIT_CODE=$?
  if [ $EXIT_CODE -ne 0 ]; then
    echo "Unable to remove the container 'heron'"
    exit 1
  fi
  echo "${bold}Heron sandbox is shutdown ${normal}"
}

check_docker_install

case $1 in
  start)
    DOCKER_IMAGE=${2:-heron/heron:latest}
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
    echo "${bold}Starting and shutting down the heron sandbox${normal}"
    echo "  "
    echo "  ${bold}Starting ${normal}the heron sandbox"
    echo "    $0 start "
    echo "  "

    echo "  ${bold}Shutting down ${normal}the heron sandbox"
    echo "    $0 stop "
    echo "  "

    echo "  ${bold}Check ${normal}if the heron sandbox is running"
    echo "    $0 ps "
    echo "  "

    echo "${bold}Topology examples in the heron sandbox ${normal}"
    echo "  "
    echo "  Heron ${bold}Java API ${normal}example topologies are available in "
    echo "    /heron/examples/heron-api-examples.jar"
    echo "  "

    echo "  Heron ${bold}Java Streamlet ${normal}example topologies are available in "
    echo "    /heron/examples/heron-streamlet-examples.jar"
    echo "  "

    echo "  Heron ${bold}Java ECO ${normal}example topologies are available in "
    echo "    /heron/examples/heron-eco-examples.jar"
    echo "  "
    
    echo "${bold}Playing with the examples in the heron sandbox ${normal}"
    echo "  "
    echo "  First ${bold}create ${normal}a shell to play with example topologies"
    echo "    $0 shell "
    echo "  "
    
    echo "  For ${bold}submitting ${normal}a topology in the shell"
    echo "    heron submit sandbox /heron/examples/heron-api-examples.jar com.twitter.heron.examples.api.ExclamationTopology exclamation"
    echo "  "
    
    echo "  For ${bold}deactivating ${normal}a topology in the shell"
    echo "    heron deactivate sandbox exclamation"
    echo "  "
    
    echo "  For ${bold}activating ${normal}a topology in the shell"
    echo "    heron activate sandbox exclamation"
    echo "  "
    
    echo "  For ${bold}killing ${normal}a topology in the shell"
    echo "    heron kill sandbox exclamation"
    echo "  "
    
    exit 1
    ;;
esac
