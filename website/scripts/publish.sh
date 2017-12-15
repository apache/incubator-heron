#!/bin/bash

ROOT_DIR=$(git rev-parse --show-toplevel)
ORIGINAL_BRANCH=$(git rev-parse --abbrev-ref HEAD)

(
    rm -rf $ROOT_DIR/website/public
    cd $ROOT_DIR
    git submodule update --init
    cd $ROOT_DIR/website/public
    git checkout gh-pages
    cd $ROOT_DIR/website
    make site && \
        cd $ROOT_DIR/website/public && \
        git commit -am "new build" && \
        git push && \
        cd $ROOT_DIR && \
        rm -rf $ROOT_DIR/website/public && \
        git submodule update --init
)
