#!/bin/bash

brew list hugo || brew install hugo
npm install
pip uninstall requests
pip install linkchecker pygments==2.1.3 requests==2.9.0
(cd assets/solarized-pygment && ./setup.py install)
rm -rf assets/solarized-pygment/{build,dist,pygments_solarized.egg-info}
