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

set -e

function die {
  echo "ERROR: $1" && exit 1;
}

TIMINGS=()
DELIM="::"

function __assert_task_name {
  [[ $1 != *"$DELIM"* ]] || die "timer task name '$1' name must not contain ::"
}

function __secs_to_readable_date {
  if [ "`uname`" == "Darwin" ]; then
    echo `date -r $1 +'%Y-%m-%d %H:%M:%S'`
  else
    echo `date -d @$1 +'%Y-%m-%d %H:%M:%S'`
  fi
}

function __secs_to_duration {
  i=$1
  ((sec=i%60, i/=60, min=i%60, hrs=i/60))
  echo $(printf "%d:%02d:%02d" $hrs $min $sec)
}

# Call start_timer "some timer name" to start a timer with that name
function start_timer {
  __assert_task_name "$1"
  local start=`date +%s`
  TIMINGS+=("${1}${DELIM}start=${start}")
  echo "===> Starting $1 at `__secs_to_readable_date $start`"
}

# Call end_timer "some timer name" to end a timer with that name
function end_timer {
  __assert_task_name $1
  local end=`date +%s`
  for ((i = 0; i < ${#TIMINGS[@]}; i++)); do
    value="${TIMINGS[$i]}"
    if [[ $value == "${1}${DELIM}start="* ]]; then
      local start=`echo $value | sed "s|${1}${DELIM}start=||g"`
    fi
  done

  [ -n "$start" ] || die "end_timer called for task '$1' before calling start_timer"

  set +e # expr returns status 1 if evalutes to 0
  duration=`expr $end - $start`
  RESP=$?
  set -e
  [[ $RESP < 2 ]] || die "Couldn't compute $end - $start"

  TIMINGS+=("${1}${DELIM}end=${end}")
  TIMINGS+=("${1}${DELIM}duration=${duration}")
  echo "===> Finished $1 at `__secs_to_readable_date $end` (`__secs_to_duration $duration`)"
}

# Prints a summary of all completed timers
function print_timer_summary {
  echo "===> Task duration summary for `caller | cut -d ' ' -f 2`"
  echo "==========================================================="
  for ((i = 0; i < ${#TIMINGS[@]}; i++)); do
    value="${TIMINGS[$i]}"
    if [[ $value == *"${DELIM}duration="* ]]; then
      key_value=`echo $value | sed "s|${DELIM}duration=|:|g"`
      task=`echo ${key_value} | cut -d ':' -f 1`
      seconds=`echo ${key_value} | cut -d ':' -f 2`
      echo -e "${task}\t`__secs_to_duration ${seconds}`"
    fi
  done
}

# Discover the platform that we are running on
function discover_platform {
  discover=`python -mplatform`
  if [[ $discover =~ ^.*centos.*$ ]]; then
    echo "centos"
  elif [[ $discover =~ ^.*Ubuntu.*$ ]]; then
    echo "ubuntu"
  elif [[ $discover =~ ^.*debian.*$ ]]; then
    echo "debian"
  elif [[ $discover =~ ^Darwin.*$ ]]; then
    echo "darwin"
  else
    mysterious=`echo $discover | awk -F- '{print $6}'`
    echo "$mysterious platform not supported"
    exit 1
  fi
}

# Check the ci environment is valid
function ci_environ {
  environ=$1
  if [[ $environ =~ travis ]]; then
    echo "travis"
  elif [[ $environ =~ applatix ]]; then
    echo "applatix"
  else
    echo "$environ ci not supported"
    exit 1
  fi
}

function pathadd {
  if [ -d "$1" ] && [[ ":$PATH:" != *":$1:"* ]]; then
    PATH="${PATH:+"$PATH:"}$1"
  fi
}

# Uncomment below to test changes:
#T="task one"
#start_timer "$T"
#sleep 1
#end_timer "$T"
#print_timer_summary
