#!/bin/bash

source ../scripts/detect_os_type.sh

PLAT_FORM=`platform`
if [ $PLAT_FORM = darwin ]; then
  brew update && brew install nvm && source $(brew --prefix nvm)/nvm.sh
  nvm install node
  curl -L https://www.npmjs.com/install.sh | sh 
  brew list hugo || brew install hugo
  which wget || brew install wget
elif [ $PLAT_FORM = ubuntu ]; then
  sudo apt-get install golang git mercurial python-pygments -y
  export GOROOT=/usr/lib/go
  export GOPATH=$HOME/go
  export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
  go get -u -v github.com/spf13/hugo
elif [ $PLAT_FORM = centos ]; then
  sudo yum -y install nodejs npm golang --enablerepo=epel
  export GOROOT=/usr/lib/go
  export GOPATH=$HOME/go
  export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
  go get -u -v github.com/spf13/hugo
fi

npm install
sudo -H pip uninstall -y Pygments
sudo -H pip install Pygments==2.1.3
