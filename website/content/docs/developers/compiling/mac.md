---
title: Building on Mac OS X
---

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
$ git clone https://github.com/apache/incubator-heron.git && cd heron
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
