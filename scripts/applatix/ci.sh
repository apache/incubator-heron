#!/bin/bash

export PATH=$PATH:/root/bin

which gcc
gcc --version

which g++
g++ --version

which python
python --version

cd /heron && USER=abc scripts/travis/ci.sh applatix
