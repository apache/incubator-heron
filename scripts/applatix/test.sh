#!/bin/bash

export PATH=$PATH:/root/bin

which gcc-4.8
which g++-4.8
which python
cd /heron && USER=abc scripts/travis/test.sh 
