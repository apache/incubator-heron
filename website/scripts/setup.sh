#!/bin/bash

SOLARIZED_PYGMENT_DIR=assets/solarized-pygment

brew list hugo || brew install hugo
npm install

pip install linkchecker pygments==2.1.3 requests==2.9.0
(cd $SOLARIZED_PYGMENT_DIR && chmod +x setup.py && ./setup.py install)
rm -rf $SOLARIZED_PYGMENT_DIR/{build,dist,pygments_solarized.egg-info}
