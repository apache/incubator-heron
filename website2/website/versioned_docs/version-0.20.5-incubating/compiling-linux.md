---
id: version-0.20.5-incubating-compiling-linux
title: Compiling on Linux
sidebar_label: Compiling on Linux
original_id: compiling-linux
---
<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

Heron can currently be built on the following Linux platforms:

* [Ubuntu 20.04](#building-on-ubuntu-20.04)
* [Rocky 8](#building-on-rocky-8)

## Building on Ubuntu 20.04

To build Heron on a fresh Ubuntu 20.04 installation:

### Step 1 --- Update Ubuntu

```bash
$ sudo apt-get update -y
$ sudo apt-get upgrade -y
```

### Step 2 --- Install required libraries

```bash
$ sudo apt-get install git build-essential automake cmake libtool-bin zip ant \
  libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python3-setuptools -y
```

#### Step 3 --- Set the following environment variables

```bash
export CC=/usr/bin/gcc
export CCX=/usr/bin/g++
```

### Step 4 --- Install JDK 11 and set JAVA_HOME

```bash
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update -y
$ sudo apt-get install openjdk-11-jdk-headless -y
$ export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-amd64"
```

#### Step 5 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/0.26.0/bazel-0.26.0-installer-linux-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

Make sure to download the appropriate version of Bazel (currently {{%
bazelVersion %}}).

### Step 6 --- Install python development tools
```bash
$ sudo apt-get install  python3-dev python3-pip
```

### Step 7 --- Make sure the Bazel executable is in your `PATH`

```bash
$ export PATH="$PATH:$HOME/bin"
```

### Step 8 --- Fetch the latest version of Heron's source code

```bash
$ git clone https://github.com/apache/incubator-heron.git && cd heron
```

### Step 9 --- Configure Heron for building with Bazel

```bash
$ ./bazel_configure.py
```

### Step 10 --- Build the project

```bash
$ bazel build heron/...
```

### Step 11 --- Build the packages

```bash
$ bazel build scripts/packages:binpkgs
$ bazel build scripts/packages:tarpkgs
```

This will install Heron packages in the `bazel-bin/scripts/packages/` directory.

## Manually Installing Libraries

If you encounter errors with [libunwind](http://www.nongnu.org/libunwind), [libtool](https://www.gnu.org/software/libtool), or
[gperftools](https://github.com/gperftools/gperftools/releases), we recommend
installing them manually.

### Compling and installing libtool

```bash
$ wget http://ftpmirror.gnu.org/libtool/libtool-2.4.6.tar.gz
$ tar -xvf libtool-2.4.6.tar.gz
$ cd libtool-2.4.6
$ ./configure
$ make
$ sudo make install
```

### Compiling and installing libunwind

```bash
$ wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
$ tar -xvf libunwind-1.1.tar.gz
$ cd libunwind-1.1
$ ./configure
$ make
$ sudo make install
```

### Compiling and installing gperftools

```bash
$ wget https://github.com/gperftools/gperftools/releases/download/gperftools-2.5/gperftools-2.5.tar.gz
$ tar -xvf gperftools-2.5.tar.gz
$ cd gperftools-2.5
$ ./configure
$ make
$ sudo make install
```

## Building on Rocky 8

To build Heron on a fresh Rocky 8 installation:

### Step 1 --- Install the required dependencies

```bash
$ sudo yum install gcc gcc-c++ kernel-devel wget unzip zlib-devel zip git automake cmake patch libtool ant pkg-config -y
```

### Step 2 --- Install libunwind from source

```bash
$ wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
$ tar xvf libunwind-1.1.tar.gz
$ cd libunwind-1.1
$ ./configure
$ make
$ sudo make install
```

### Step 3 --- Set the following environment variables

```bash
$ export CC=/usr/bin/gcc
$ export CCX=/usr/bin/g++
```

### Step 4 --- Install JDK 11

```bash
$ sudo yum install java-11-openjdk java-11-openjdk-devel
$ export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
```

#### Step 5 - Install Bazelisk

Bazelisk helps automate the management of Bazel versions

```bash
wget -O /tmp/bazelisk https://github.com/bazelbuild/bazelisk/releases/download/v1.11.0/bazelisk-darwin-amd64
chmod +x /tmp/bazelisk
sudo mv /tmp/bazelisk /usr/local/bin/bazel
```

### Step 6 --- Fetch the latest version of Heron's source code

```bash
$ git clone https://github.com/apache/incubator-heron.git && cd incubator-heron
```


### Step 7 --- Configure Heron for building with Bazel

```bash
$ ./bazel_configure.py
```

### Step 8 --- Build the project

```bash
$ bazel build heron/...
```

This will build in the Bazel default `fastbuild` mode. Production release packages include additional performance optimizations not enabled by default. To enable production optimizations, include the `opt` flag. This defaults to optimization level `-O2`. The second option overrides the setting to bump it to `-CO3`.

```bash
$ bazel build -c opt heron/...
```

```bash
$ bazel build -c opt --copt=-O3 heron/...
```

If you wish to add the code syntax style check, add `--config=stylecheck`.

### Step 9 --- Build the binary packages

```bash
$ bazel build scripts/packages:binpkgs
$ bazel build scripts/packages:tarpkgs
```

This will install Heron packages in the `bazel-bin/scripts/packages/` directory.
