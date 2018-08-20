#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

DIR=`dirname $0`
source ${DIR}/../../scripts/detect_os_type.sh

PLATFORM=`platform`
if [ $PLATFORM = darwin ]; then
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

npm install
sudo -H pip uninstall -y pygments
sudo -H pip install pygments==2.1.3 pdoc==0.3.2
