---
id: version-0.20.0-incubating-compiling-osx
title: Compiling on OS X
sidebar_label: Compiling on OS X
original_id: compiling-osx
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

### Step 2 --- Install bazel and other required libraries

```bash
brew install bazel
brew install automake
brew install cmake
brew install libtool
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
$ bazel build --config=darwin heron/...
```

### Step 7 --- Build the packages

```bash
$ bazel build --config=darwin scripts/packages:binpkgs
$ bazel build --config=darwin scripts/packages:tarpkgs
```

This will install Heron packages in the `bazel-bin/scripts/packages/` directory.
