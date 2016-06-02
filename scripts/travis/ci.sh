#!/bin/bash

set -e

DIR=`dirname $0`

source ${DIR}/common.sh

start_timer "make site"
(cd website && make site)
end_timer "make site"

start_timer "${DIR}/build.sh"
${DIR}/build.sh
end_timer "${DIR}/build.sh"

start_timer "${DIR}/test.sh"
${DIR}/test.sh
end_timer "${DIR}/test.sh"

print_timer_summary
