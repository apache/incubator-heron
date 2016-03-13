#!/usr/bin/env bash
set -e
set -x

packer add_version --cluster=smf1 heron heron-ui $2
aurora deploy create smf1/heron/prod/heron-ui $3 -r
