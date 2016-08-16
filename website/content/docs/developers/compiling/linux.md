---
title: Linux Platforms
---

* [Ubuntu 14.04]({{< ref "#building-on-ubuntu-14.04" >}})
* [CentOS 7]({{< ref "#building-on-centos-7" >}})

## Building on Ubuntu 14.04

This is a step by step guide for building Heron on a fresh Ubuntu 14.04 installation. 

#### Step 1 - First update Ubuntu.

```bash
sudo apt-get update -y
sudo apt-get upgrade -y
```

#### Step 2 - Install required libraries

```bash
sudo apt-get install git build-essential automake cmake libtool zip \ 
        libunwind-setjmp0-dev zlib1g-dev unzip pkg-config -y
```

#### Step 3 - Set the following environment variables

```bash
export CC=/usr/bin/gcc-4.8
export CCX=/usr/bin/g++-4.8
```

#### Step 4 - Install JDK 8

```bash
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update -y
sudo apt-get install oracle-java8-installer -y
```

#### Step 5 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/0.2.3/bazel-0.2.3-installer-linux-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

#### Step 6 - Make sure Bazel bin is in the PATH

```bash
export PATH="$PATH:$HOME/bin"
```

#### Step 7 - Get the latest version of heron

```bash
git clone https://github.com/twitter/heron.git && cd heron
```

#### Step 8 - Configure Heron for build

```bash
./bazel_configure.py
```

#### Step 9 - Build the project

```bash
bazel build --config=ubuntu heron/...  
```

#### Step 10 - Build the packages

```bash
bazel build --config=ubuntu scripts/packages:binpkgs  
bazel build --config=ubuntu scripts/packages:tarpkgs
```

This will build the packages below the `bazel-bin/scripts/packages/` directory. 

### Manually Installing Libraries

If you encounter errors with libunwind, libtool, or gperftools install them manually

Compiling and installing [libtool] (https://www.gnu.org/software/libtool)
```bash
wget http://ftpmirror.gnu.org/libtool/libtool-2.4.6.tar.gz
tar -xvf libtool-2.4.6.tar.gz
cd libtool-2.4.6
./configure
make
sudo make install
```

Compiling and installing [libunwind] (http://www.nongnu.org/libunwind)
```bash
wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
tar -xvf libunwind-1.1.tar.gz
cd libunwind-1.1
./configure
make
sudo make install
```

Compiling and installing [gperftools] (https://github.com/gperftools/gperftools/releases)

```bash
wget https://github.com/gperftools/gperftools/releases/download/gperftools-2.5/gperftools-2.5.tar.gz
tar -xvf gperftools-2.5.tar.gz
cd gperftools-2.5
./configure
make
sudo make install
```

## Building on CentOS 7

This is a step by step guide for building Heron on a fresh CentOS 7 installation.

#### Step 1 - Install the required dependencies

```bash
sudo yum install gcc gcc-c++ kernel-devel wget unzip zlib-devel zip git automake cmake patch libtool -y
```

#### Step 2 - Install libunwind from source

```bash
wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
tar xvf libunwind-1.1.tar.gz
cd libunwind-1.1
./configure
make
sudo make install
```

#### Step 3 - Set the following environment variables

```bash
export CC=/usr/bin/gcc
export CCX=/usr/bin/g++
```

#### Step 4 - Install JDK

```bash
cd /opt/
sudo wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u91-b14/jdk-8u91-linux-x64.tar.gz"

sudo tar xzf jdk-8u91-linux-x64.tar.gz
```

Use alternatives to configure the Java version

```bash
sudo cd /opt/jdk1.8.0_91/
sudo alternatives --install /usr/bin/java java /opt/jdk1.8.0_91/bin/java 2
sudo alternatives --config java
```

Set the javac and jar commands

```bash
sudo alternatives --install /usr/bin/jar jar /opt/jdk1.8.0_91/bin/jar 2
sudo alternatives --install /usr/bin/javac javac /opt/jdk1.8.0_91/bin/javac 2
sudo alternatives --set jar /opt/jdk1.8.0_91/bin/jar
sudo alternatives --set javac /opt/jdk1.8.0_91/bin/javac
```

Export the Java environment variables

```bash
export JAVA_HOME=/opt/jdk1.8.0_91
export JRE_HOME=/opt/jdk1.8.0_91/jre
export PATH=$PATH:/opt/jdk1.8.0_91/bin:/opt/jdk1.8.0_91/jre/bin
```

#### Step 5 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh https://github.com/bazelbuild/bazel/releases/download/0.2.3/bazel-0.2.3-installer-linux-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

#### Step 6 - Download heron and compile it

```bash
cd
git clone https://github.com/twitter/heron.git && cd heron
./bazel_configure.py
bazel build --config=centos heron/...
```

#### Step 7 - Build the binary packages

```bash
bazel build --config=centos scripts/packages:binpkgs
bazel build --config=centos scripts/packages:tarpkgs
```

This will build the packages below the `bazel-bin/scripts/packages/` directory.


