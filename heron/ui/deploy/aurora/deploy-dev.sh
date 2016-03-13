#!/usr/bin/env bash
set -e
set -x

packer add_version --cluster=smf1 ${USER} heron-ui $2
aurora deploy create smf1/${USER}/devel/heron-ui $3 -r
open http://heron-ui.devel.${USER}.service.smf1.twitter.com
