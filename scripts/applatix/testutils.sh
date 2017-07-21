#!/bin/bash
#
# Script to kick off the travis CI integration test. Fail-fast if any of tthe below commands fail.
#
set -e

DIR=`dirname $0`
UTILS=${DIR}/../shutils
source ${UTILS}/common.sh

# Autodiscover the platform
PLATFORM=$(discover_platform)
echo "Using $PLATFORM platform"

# include HOME directory bin in PATH for heron cli, tools and tests
export PATH=${HOME}/bin:$PATH

# install client
T="heron client install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_client_install.txt" ./heron-client-install.sh --user
end_timer "$T"

# install tools
T="heron tools install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_tools_install.txt" ./heron-tools-install.sh --user
end_timer "$T"

# install tests
T="heron tests install"
start_timer "$T"
python ${UTILS}/save-logs.py "heron_tests_install.txt" ./heron-tests-install.sh --user
end_timer "$T"

print_timer_summary
