#!/bin/bash

docker build -f Dockerfile.build -t get.applatix.io/heron-build:v1.0 .
docker push get.applatix.io/heron-build:v1.0
