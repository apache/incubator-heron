---
title: Compiling Heron
---

Heron is currently available for [Mac OS X 10.10](../../../developers/compiling/mac),
[Ubuntu 14.04](../../../developers/compiling/linux), and [CentOS
7](../../../developers/compiling/linux). This guide describes the basics of the
Heron build system. For step-by-step build instructions for a specific platform,
the following guides are available:

* [Building on Linux Platforms](../../../developers/compiling/linux)
* [Building on Mac OS X](../../../developers/compiling/mac)

Heron can be built either [in its entirety]({{< ref "#building-all-components"
>}}), as [individual components]({{< ref "#building-specific-components" >}}),
or as a [release package]({{< ref "#building-a-full-release-package" >}}).

Instructions on running unit tests for Heron can also be found in [Testing Heron](../../../contributors/testing).

## Requirements

You must have the following installed to compile Heron:

* [Bazel](http://bazel.io/docs/install.html) = {{% bazelVersion %}}. Later
  versions might work but have not been tested. See [Installing Bazel]({{< ref
  "#installing-bazel" >}}) below.
* [Java
  8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
  is required by Bazel but Heron does not use Java 8 features; Heron
  [topologies](../../../concepts/topologies) can be written in Java 7 or above
  and all Heron jars are compatible with Java 7.
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
Tracker](../../../operators/heron-tracker)) by passing a target to the `bazel
build` command. For example, the following command would build the [Heron Tracker](../../../operators/heron-tracker):

```bash
$ bazel build --config=darwin heron/tools/tracker/src/python:heron-tracker
```

## Testing Heron

Instructions for running Heron unit tests can be found at [Testing
Heron](../../../contributors/testing).
