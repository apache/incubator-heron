#!/bin/bash

brew list hugo || brew install hugo
npm install
sudo -H pip uninstall -y Pygments
sudo -H pip install Pygments==2.1.3
