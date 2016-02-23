#!/usr/bin/env bash

dist_dir=/vagrant/contrib/kafka9/vagrant/heron-ubuntu

rm -rf ${dist_dir}/heron-0.1.0-SNAPSHOT

mkdir -p ${dist_dir}/heron-0.1.0-SNAPSHOT

tar -vxf ${dist_dir}/heron-0.1.0-SNAPSHOT.tar.gz -C ${dist_dir}/heron-0.1.0-SNAPSHOT

cp ${dist_dir}/heron-0.1.0-SNAPSHOT/conf/* ${dist_dir}/heron-0.1.0-SNAPSHOT