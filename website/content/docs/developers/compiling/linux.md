---
title: Linux Platforms
---

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

#### Step 5 - Install Bazel

```bash
wget https://github.com/bazelbuild/bazel/releases/download/0.1.2/bazel-0.1.2-installer-linux-x86_64.sh
chmod +x bazel-0.1.2-installer-linux-x86_64.sh
./bazel-0.1.2-installer-linux-x86_64.sh --user
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
