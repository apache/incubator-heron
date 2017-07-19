#!/bin/bash

ROOT_DIR=$(git rev-parse --show-topleveo)

(
    cd $ROOT_DIR
    rm -rf website/public
    git submodule update --init
    cd website/public && git checkout gh-pages && git remote rename origin upstream && cd $ROOT_DIR
    cd website && make travis-site && cd $ROOT_DIR
    cd website/public
    git add -A && git commit -m "New website build at $(date)"
    git push -q upstream HEAD:gh-pages
)
