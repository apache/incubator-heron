#!/bin/bash

brew list hugo || brew install hugo
npm install
sudo -H pip uninstall -y requests
sudo -H pip install linkchecker pygments requests==2.9.0
