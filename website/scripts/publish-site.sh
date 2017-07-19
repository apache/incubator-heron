#!/bin/bash

ROOT_DIR=$(git rev-parse --show-topleveo)

(
    cd $ROOT_DIR
    rm -rf website/public
    git submodule update --init
    (cd website/public && git checkout gh-pages)
    (cd website && make travis-site)
    cd website/public
    git add -A && git commit -m "New website build at $(date)"
    git push -u origin gh-pages
)
