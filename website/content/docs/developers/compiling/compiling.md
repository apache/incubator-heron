---
title: Compiling Heron
---

Heron is currently available for [Mac OS X 10.10](../../../developers/compiling/mac), 
[Ubuntu 12.04, Ubuntu 14.04](../../../developers/compiling/linux),
and [CentOS 7](../../../developers/compiling/linux).
This guide describes the basics of Heron build system. For step by step build instructions
on a specific platform refer the following guides.

* [Build on Linux](../../../developers/compiling/linux)
* [Build on Mac OS X](../../../developers/compiling/mac)

Heron can be built either [in it's entirety]({{< ref "#building-all-components" >}}), as
[individual components]({{< ref "#building-specific-components" >}}), or as a [release
package]({{< ref "#building-a-full-release-package" >}}).

Instructions on running unit tests for Heron can also be found in [Testing Heron](../../../contributors/testing).

## Requirements

You must have the following installed to compile Heron:

* [Bazel](http://bazel.io/docs/install.html) = {{% bazelVersion %}} . Later versions
  might work but have not been tested. See [Installing Bazel]({{< ref "#installing-bazel" >}}).
* [Java
  8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
  is required by Bazel but Heron does not use Java 8 features;
  [topologies](../../../concepts/topologies) can be written in Java 7 or above and all
  Heron jars are compatible with Java 7.
* [Autoconf](http://www.gnu.org/software/autoconf/autoconf.html) >=
  2.6.3
* [Automake](https://www.gnu.org/software/automake/) >= 1.11.1
* [GNU Make](https://www.gnu.org/software/make/) >= 3.81
* [GNU Libtool](http://www.gnu.org/software/libtool/) >= 2.4.6
* [gcc/g++](https://gcc.gnu.org/) >= 4.8.1 (Linux platforms)
* [CMake](https://cmake.org/) >= 2.6.4
* [Python](https://www.python.org/) >= 2.7 (not including Python 3.x)
* [Perl](https://www.perl.org/) >= 5.8.8

Export CC and CXX variables with path specific to your machine:

```bash
$ export CC=/your-path-to/bin/c_compiler
$ export CXX=/your-path-to/bin/c++_compiler
$ echo $CC $CXX
```

## Installing Bazel

Heron uses the [Bazel](http://bazel.io) build tool. Bazel releases can be found
[here](https://github.com/bazelbuild/bazel/releases/tag/{{% bazelVersion %}})
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
* `ubuntu` (Ubuntu 14.04)
* `centos5` (CentOS 5)

For example, on Darwin, the following command will build all packages:

```bash
$ bazel build --config=darwin heron/...
```

### Building All Components

The bazel build process can produce either executable install scripts, or
bundled tars. To build executables or tars for all Heron components at once,
use the following `bazel build` commands, respectively.

```bash
$ bazel build --config=darwin scripts/packages:binpkgs
$ bazel build --config=darwin scripts/packages:tarpkgs
```

Resulting artifacts can be found in subdirectories below the `bazel-bin`
directory. The `heron-tracker` executable, for example, can be found at
`bazel-bin/heron/tracker/src/python/heron-tracker`.

### Building Specific Components

As an alternative to building a full release, you can build Heron executables for
a single component by passing a target to the `bazel build` command. For example
the following command will build the [Heron Tracker](../../../operators/heron-tracker):

```bash
$ bazel build --config=darwin heron/tracker/src/python:heron-tracker
```

## Testing Heron

Instructions for running Heron unit tests can be found at [Testing Heron](../../../contributors/testing).

