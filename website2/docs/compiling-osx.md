---
id: compiling-osx
title: Compiling on OS X
sidebar_label: Compiling on OS X
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

This is a step-by-step guide to building Heron on Mac OS X (versions 10.10 and
  10.11).

### Step 1 --- Install Homebrew

If [Homebrew](http://brew.sh/) isn't yet installed on your system, you can
install it using this one-liner:

```bash
$ /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

### Step 2 -- Install Bazelisk

Bazelisk helps automate the management of Bazel versions

```bash
brew install bazelisk
```

### Step 2 --- Install other required libraries

```bash
brew install automake
brew install cmake
brew install libtool
brew install ant
brew install pkg-config
```

### Step 3 --- Set the following environment variables

```bash
$ export CC=/usr/bin/clang
$ export CXX=/usr/bin/clang++
$ echo $CC $CXX
```

### Step 4 --- Fetch the latest version of Heron's source code

```bash
$ git clone https://github.com/apache/incubator-heron.git && cd incubator-heron
```

### Step 5 --- Configure Heron for building with Bazel

```bash
$ ./bazel_configure.py
```

If this configure script fails with missing dependencies, Homebrew can be used
to install those dependencies.

### Step 6 --- Build the project

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

### Step 7 --- Build the packages

```bash
$ bazel build scripts/packages:binpkgs
$ bazel build scripts/packages:tarpkgs
```

This will install Heron packages in the `bazel-bin/scripts/packages/` directory.
