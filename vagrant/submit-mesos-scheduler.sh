#!/usr/bin/env bash

curl -X POST -H "Content-Type: application/json" --data @mesos-scheduler.json http://master:8080/v2/apps