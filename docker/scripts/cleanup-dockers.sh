#!/bin/bash
set -o nounset
set -o errexit

# remove the existing docker containers
docker rm -v $(docker ps -a -q -f status=exited)

# remove the dangling the docker images
docker rmi $(docker images -f "dangling=true" -q)
