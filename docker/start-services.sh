#!/bin/bash

heron-tracker --rootpath ~/.herondata/repository/state/onenode > /var/log/heron-tracker.log 2>&1 &
heron-ui > /var/log/heron-ui.log 2>&1 &
heron-apiserver --base-template=local --cluster=onenode -Dheron.directory.home=/usr/local/heron > /var/log/heron-apiserver.log 2>&1 &
sleep infinity
