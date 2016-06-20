#!/bin/bash

brew list hugo || brew install hugo
which wget || brew install wget
npm install
sudo -H pip uninstall -y Pygments
sudo -H pip install Pygments==2.1.3
