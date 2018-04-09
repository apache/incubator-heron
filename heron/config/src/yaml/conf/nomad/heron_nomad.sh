#!/bin/sh

set -e
set -x

CORE_RELEASE_FILE="heron-core.tar.gz"
CORE_RELEASE_DIR="heron-core"
TOPOLOGY_PACKAGE_FILE="topology.tar.gz"

# remove directory if already exists
if [ -d "$HERON_NOMAD_WORKING_DIR" ]; then
  rm -rf $HERON_NOMAD_WORKING_DIR
fi

# Create working directory if it does not exist
mkdir -p ${HERON_NOMAD_WORKING_DIR}

# Go to working directory
cd ${HERON_NOMAD_WORKING_DIR}

if [ "$HERON_USE_CORE_PACKAGE_URI" == "true" ]; then
  # download and extract heron core package
  curl ${HERON_CORE_PACKAGE_URI} -o ${CORE_RELEASE_FILE}
  tar zxf ${CORE_RELEASE_FILE} && rm -rf ${CORE_RELEASE_FILE}
else
  # link the heron core package directory
  ln -s ${HERON_CORE_PACKAGE_DIR} ${CORE_RELEASE_DIR}
fi

# download and extract heron topology package
${HERON_TOPOLOGY_DOWNLOAD_CMD}

# set metrics port file
echo ${NOMAD_PORT_metrics_port} > ${METRICS_PORT_FILE}

# launch heron executor
trap 'kill -TERM $PID' TERM INT
${HERON_EXECUTOR_CMD} &
PID=$!
wait $PID
trap - TERM INT
wait $PID
EXIT_STATUS=$?
