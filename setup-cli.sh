#!/usr/bin/env bash

if [[ $# -ne 1 ]] ; then
    echo 'USAGE: ./setup-cli.sh <dist_dir>'
    exit 1
fi

rm -rf $1/heron-0.1.0-SNAPSHOT

mkdir -p $1/heron-0.1.0-SNAPSHOT

tar -vxf $1/heron-0.1.0-SNAPSHOT.tar.gz -C $1/heron-0.1.0-SNAPSHOT

cp $1/heron-0.1.0-SNAPSHOT/conf/* $1/heron-0.1.0-SNAPSHOT