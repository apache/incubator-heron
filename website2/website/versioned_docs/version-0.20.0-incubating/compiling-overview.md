---
id: version-0.20.0-incubating-compiling-overview
title: Compiling Heron
sidebar_label: Compiling Overview
original_id: compiling-overview
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

Heron is currently available for [Mac OS X 10.14](compiling-osx),
[Ubuntu 18.04](compiling-linux), and [Debian10](compiling-docker#building-heron).
 This guide describes the basics of the
Heron build system. For step-by-step build instructions for other platforms,
the following guides are available:

* [Building on Linux Platforms](compiling-linux)
* [Building on Mac OS X](compiling-osx)

Heron can be built either [in its entirety](#building-all-components), as [individual components](#building-specific-components).

Instructions on running unit tests for Heron can also be found in [Testing Heron](compiling-running-tests).

## Requirements

You must have the following installed to compile Heron:

* [Bazel](http://bazel.io/docs/install.html) = {{% bazelVersion %}}. Later
  versions might work but have not been tested. See [Installing Bazel](#installing-bazel)below.
* [Java 11](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
  is required by Bazel and Heron;
  topologies can be written in Java 7 or above
  , but Heron jars are required to run with a Java 11 JRE.
* [Autoconf](http://www.gnu.org/software/autoconf/autoconf.html) >=
  2.6.3
* [Automake](https://www.gnu.org/software/automake/) >= 1.11.1
* [GNU Make](https://www.gnu.org/software/make/) >= 3.81
* [GNU Libtool](http://www.gnu.org/software/libtool/) >= 2.4.6
* [gcc/g++](https://gcc.gnu.org/) >= 4.8.1 (Linux platforms)
* [CMake](https://cmake.org/) >= 2.6.4
* [Python](https://www.python.org/) >= 2.7 (not including Python 3.x)
* [Perl](https://www.perl.org/) >= 5.8.8

Export the `CC` and `CXX` environment variables with a path specific to your
machine:

```bash
$ export CC=/your-path-to/bin/c_compiler
$ export CXX=/your-path-to/bin/c++_compiler
$ echo $CC $CXX
```

## Installing Bazel

Heron uses the [Bazel](http://bazel.io) build tool. Bazel releases can be found here:
https://github.com/bazelbuild/bazel/releases/tag/{{% bazelVersion %}}
and installation instructions can be found [here](http://bazel.io/docs/install.html).

To ensure that Bazel has been installed, run `bazel version` and check the
version (listed next to `Build label` in the script's output) to ensure that you
have Bazel {{% bazelVersion %}}.

## Configuring Bazel

There is a Python script that you can run to configure Bazel on supported
platforms:

```bash
$ cd /path/to/heron
$ ./bazel_configure.py
```

## Building

### Bazel OS Environments

Bazel builds are specific to a given OS. When building you must specify an
OS-specific configuration using the `--config` flag. The following OS values
are supported:

* `darwin` (Mac OS X)
* `ubuntu` (Ubuntu 18.04)
* `debian` (Debian10)
* `centos5` (CentOS 7)

For example, on Mac OS X (`darwin`), the following command will build all
packages:

```bash
$ bazel build --config=darwin heron/...
```

Production release packages include additional performance optimizations
not enabled by default. Enabling these optimizations increases build time.
To enable production optimizations, include the `opt` flag:
```bash
$ bazel build -c opt --config=PLATFORM heron/...
```

### Building All Components

The Bazel build process can produce either executable install scripts or
bundled tars. To build executables or tars for all Heron components at once,
use the following `bazel build` commands, respectively:

```bash
$ bazel build --config=PLATFORM scripts/packages:binpkgs
$ bazel build --config=PLATFORM scripts/packages:tarpkgs
```

Resulting artifacts can be found in subdirectories below the `bazel-bin`
directory. The `heron-tracker` executable, for example, can be found at
`bazel-bin/heron/tools/tracker/src/python/heron-tracker`.

### Building Specific Components

As an alternative to building a full release, you can build Heron executables
for a single Heron component (such as the [Heron
Tracker](user-manuals-heron-tracker-runbook)) by passing a target to the `bazel
build` command. For example, the following command would build the Heron Tracker:

```bash
$ bazel build --config=darwin heron/tools/tracker/src/python:heron-tracker
```

## Testing Heron

Instructions for running Heron unit tests can be found at [Testing
Heron](compiling-running-tests).
