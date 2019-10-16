---
id: version-0.20.0-incubating-compiling-linux
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

* [Ubuntu 14.04](#building-on-ubuntu-14.04)
* [CentOS 7](#building-on-centos-7)

## Building on Ubuntu 14.04

To build Heron on a fresh Ubuntu 14.04 installation:

### Step 1 --- Update Ubuntu

```bash
$ sudo apt-get update -y
$ sudo apt-get upgrade -y
```

### Step 2 --- Install required libraries

```bash
$ sudo apt-get install git build-essential automake cmake libtool-bin zip \
  libunwind-setjmp0-dev zlib1g-dev unzip pkg-config python-setuptools -y
```

#### Step 3 --- Set the following environment variables

```bash
export CC=/usr/bin/gcc
export CCX=/usr/bin/g++
```

### Step 4 --- Install JDK 8 and set JAVA_HOME

```bash
$ sudo add-apt-repository ppa:webupd8team/java
$ sudo apt-get update -y
$ sudo apt-get install oracle-java8-installer -y
$ export JAVA_HOME="/usr/lib/jvm/java-8-oracle"
```

#### Step 5 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/0.23.2/bazel-0.23.2-installer-linux-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

Make sure to download the appropriate version of Bazel (currently {{%
bazelVersion %}}).

### Step 6 --- Install python development tools
```bash
$ sudo apt-get install  python-dev python-pip
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
$ bazel build --config=ubuntu heron/...  
```

### Step 11 --- Build the packages

```bash
$ bazel build --config=ubuntu scripts/packages:binpkgs  
$ bazel build --config=ubuntu scripts/packages:tarpkgs
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

## Building on CentOS 7

To build Heron on a fresh CentOS 7 installation:

### Step 1 --- Install the required dependencies

```bash
$ sudo yum install gcc gcc-c++ kernel-devel wget unzip zlib-devel zip git automake cmake patch libtool -y
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

### Step 4 --- Install JDK 8

```bash
$ cd /opt/
$ sudo wget --no-cookies --no-check-certificate \
  --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" \
  "http://download.oracle.com/otn-pub/java/jdk/8u91-b14/jdk-8u91-linux-x64.tar.gz"
$ sudo tar xzf jdk-8u91-linux-x64.tar.gz
```

Use `alternatives` to configure the Java version:

```bash
$ sudo cd /opt/jdk1.8.0_91/
$ sudo alternatives --install /usr/bin/java java /opt/jdk1.8.0_91/bin/java 2
$ sudo alternatives --config java
```

Set the `javac` and `jar` commands:

```bash
$ sudo alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_91/bin/jar 2
$ sudo alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_91/bin/javac 2
$ sudo alternatives --set jar /opt/jdk1.8.0_91/bin/jar
$ sudo alternatives --set javac /opt/jdk1.8.0_91/bin/javac
```

Export Java-related environment variables:

```bash
export JAVA_HOME=/opt/jdk1.8.0_91
export JRE_HOME=/opt/jdk1.8.0_91/jre
export PATH=$PATH:/opt/jdk1.8.0_91/bin:/opt/jdk1.8.0_91/jre/bin
```

#### Step 5 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/0.23.2/bazel-0.23.2-installer-linux-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

Make sure to download the appropriate version of Bazel (currently {{%
bazelVersion %}}).

### Step 6 --- Download Heron and compile it

```bash
$ git clone https://github.com/apache/incubator-heron.git && cd heron
$ ./bazel_configure.py
$ bazel build --config=centos heron/...
```

### Step 7 --- Build the binary packages

```bash
$ bazel build --config=centos scripts/packages:binpkgs
$ bazel build --config=centos scripts/packages:tarpkgs
```

This will install Heron packages in the `bazel-bin/scripts/packages/` directory.
