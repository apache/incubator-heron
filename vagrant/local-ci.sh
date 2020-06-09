#!/usr/bin/env bash
:<<'DOC'
This script is for running tests in a local VM, similar to the environment used in the CI pipeline. If the targent script fails, a shell will be opened up within the VM.

To run all of the tests:
  ./local-ci.sh

To run a specific command/script:
  ./local-ci.sh ./scripts/travis/ci.sh
  ./local-ci.sh bash

The VM does not report the platform in python as expected, so PLATFORM=Ubuntu is needed to work around that for the CI script's platform discovery.

DOC

set -o errexit -o nounset -o pipefail
HERE="$(cd "$(dirname "$0")" && pwd -P)"

cd "$HERE"

state="$(vagrant status master --machine-readable | grep master,state, | cut -d, -f4)"
if [ "$state" != "running" ]; then
    vagrant up master
fi


if [ -z "$1" ]; then
    # run the CI, if it fails drop into a shell
    vagrant ssh master --command "cd /vagrant && $env ./scripts/travis/$script.sh" \
        || vagrant ssh master --command "cd /vagrant && exec bash"
else
    vagrant ssh master --command "cd /vagrant && exec $1"
fi
