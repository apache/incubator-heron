#!/bin/bash

brew list hugo || brew install hugo
npm install
pip uninstall requests
pip install linkchecker pygments==2.1.3 requests==2.9.0 pygments-solarized==0.2
