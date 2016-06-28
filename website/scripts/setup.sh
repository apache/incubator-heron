#!/bin/bash

DIR=`dirname $0`
source ${DIR}/../../scripts/detect_os_type.sh

PLATFORM=`platform`
if [ $PLATFORM = darwin ]; then
  brew update && brew install nvm && source $(brew --prefix nvm)/nvm.sh
  nvm install node
  curl -L https://www.npmjs.com/install.sh | sh 
  brew list hugo || brew install hugo
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

npm install
sudo -H pip uninstall -y pygments
sudo -H pip install pygments==2.1.3
