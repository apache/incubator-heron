#!/bin/bash

HERON_ROOT_DIR=$(git rev-parse --show-toplevel)
BUNDLER_VERSION=1.15.1
HUGO_VERSION=0.25.1

gem install bundler \
  -v $BUNDLER_VERSION \
  --no-rdoc \
  --no-ri
NOKOGIRI_USE_SYSTEM_LIBRARIES=true bundle _${BUNDLER_VERSION}_ install \
  --path vendor/bundle

npm install
sudo -H pip uninstall -y pygments
sudo -H pip install pygments==2.1.3 pdoc==0.3.2


source $HERON_ROOT_DIR/scripts/detect_os_type.sh

PLATFORM=`platform`
if [ $PLATFORM = darwin ]; then
  brew update && brew install nvm && source $(brew --prefix nvm)/nvm.sh
  nvm install node
  curl -L https://www.npmjs.com/install.sh | sh
  go get -v github.com/gohugoio/hugo
  which wget || brew install wget
elif [ $PLATFORM = ubuntu ]; then
  sudo apt-get install golang git mercurial -y
  export GOROOT=/usr/lib/go
  export GOPATH=$HOME/go
  export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
  go get -u -v github.com/spf13/hugo
elif [ $PLATFORM = centos ]; then
  sudo yum -y install nodejs npm golang --enablerepo=epel
  export GOROOT=/usr/lib/go
  export GOPATH=$HOME/go
  export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
  go get -u -v github.com/spf13/hugo
fi
