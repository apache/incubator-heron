---
title: Building on Mac OS X
---

This is a step by step guide for building Heron on Mac OS (10.10 and 10.11).

#### Step 1 - Install brew, if already not installed

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

#### Step 2 - Install the required libraries

```bash
brew install automake
brew install cmake
brew install libtool
```

#### Step 3 - Set the following environment variables

```bash
$ export CC=/usr/bin/clang
$ export CXX=/usr/bin/clang++
$ echo $CC $CXX
```

#### Step 4 - Install Bazel {{% bazelVersion %}}

```bash
wget -O /tmp/bazel.sh  https://github.com/bazelbuild/bazel/releases/download/0.2.3/bazel-0.2.3-installer-darwin-x86_64.sh
chmod +x /tmp/bazel.sh
/tmp/bazel.sh --user
```

#### Step 5 - Make sure Bazel bin is in the PATH

```bash
export PATH="$PATH:$HOME/bin"
```

#### Step 6 - Get the latest version of heron

```bash
git clone https://github.com/twitter/heron.git && cd heron
```

#### Step 7 - Configure Heron for build

```bash
./bazel_configure.py
```

If the configure scripts fails with missing dependencies, brew can be used to install the dependencies.

#### Step 8 - Build the project

```bash
bazel build --config=darwin heron/...
```

#### Step 9 - Build the packages

```bash
bazel build --config=darwin scripts/packages:binpkgs
bazel build --config=darwin scripts/packages:tarpkgs
```

This will build the packages below the `bazel-bin/scripts/packages/` directory. 




