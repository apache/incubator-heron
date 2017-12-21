#!/bin/sh

set -e
set -x

CORE_RELEASE_FILE="heron-core.tar.gz"
TOPOLOGY_PACKAGE_FILE="topology.tar.gz"

# Create working directory if it does not exist
mkdir -p ${HERON_NOMAD_WORKING_DIR}

# Go to working directory
cd ${HERON_NOMAD_WORKING_DIR}

# download and extract heron core package
curl ${HERON_CORE_PACKAGE_URI} -o ${CORE_RELEASE_FILE}
tar zxf ${CORE_RELEASE_FILE}

#download and extract heron topology package
${HERON_TOPOLOGY_DOWNLOAD_CMD}

# launch heron executor
trap 'kill -TERM $PID' TERM INT
${HERON_EXECUTOR_CMD} &
PID=$!
wait $PID
trap - TERM INT
wait $PID
EXIT_STATUS=$?
