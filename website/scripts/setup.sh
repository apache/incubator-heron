#!/bin/bash

brew list hugo || brew install hugo
npm install
sudo -H pip uninstall -y requests Pygments
sudo -H pip install linkchecker Pygments==2.1.3 requests==2.9.0
