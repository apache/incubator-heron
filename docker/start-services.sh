#!/bin/bash

heron-tracker --rootpath ~/.herondata/repository/state/onenode > /var/log/heron-tracker.log 2>&1 &
heron-ui > /var/log/heron-ui.log 2>&1 &
heron-apiserver --base-template=local --cluster=onenode -Dheron.directory.home=/usr/local/heron > /var/log/heron-apiserver.log 2>&1 &
sleep 5
heron submit onenode --service-url http://localhost:9000 /usr/local/heron/examples/heron-examples.jar com.twitter.heron.examples.ExclamationTopology ExclamationTopology
heron submit onenode --service-url http://localhost:9000 /usr/local/heron/examples/heron-examples.jar com.twitter.heron.examples.MultiSpoutExclamationTopology MultiSpoutExclamationTopology
sleep infinity
