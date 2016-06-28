#!/bin/bash


HERON_IMAGE=${1:-heron:local-ubuntu1404}
export HERON_IMAGE

echo "Launching heron tracker, ui, and local executor env with docker-compose"
echo "    using image $HERON_IMAGE..."
echo "(only sample topologies will be available for submission; mount additional volumes to heron-executor to make additional topologies available for submission)"
echo "  "

docker-compose -f docker/docker-compose.yml -p heron up -d

echo "Example topologies are available in /usr/local/heron/examples/heron-examples.jar"
echo "  "

echo "To submit a topology:"
echo "        docker exec heron_executor_1 heron submit local /usr/local/heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology --deploy-deactivated"
echo "To activate a topology:"
echo "        docker exec -it heron_executor_1 heron activate local ExclamationTopology"
echo "To kill a topology:"
echo "        docker exec -it heron_executor_1 heron kill local ExclamationTopology"
echo "-------------------"
echo "To shutdown heron daemons: docker-compose -p heron stop"
echo "-------------------"

