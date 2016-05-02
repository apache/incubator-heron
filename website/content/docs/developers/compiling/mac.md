---
title: Max OS X (10.10)
---

Install brew

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Install the required libraries

```bash
brew install automake
brew install cmake
```

Set the following environment variables

```bash
$ export CC=/usr/bin/clang
$ export CXX=/usr/bin/clang++
$ echo $CC $CXX
```

Install Bazel

```bash
curl -O -L https://github.com/bazelbuild/bazel/releases/download/0.1.2/bazel-0.1.2-installer-darwin-x86_64.sh
chmod +x bazel-0.1.2-installer-linux-x86_64.sh
./bazel-0.1.2-installer-linux-x86_64.sh --user
```

Make sure Bazel bin is in the PATH

```bash
export PATH="$PATH:$HOME/bin"
```

Get the latest version of heron

```bash
git clone https://github.com/twitter/heron.git && cd heron
```

Configure Heron for build

```bash
./bazel_configure.py
```

If the configure scripts fails with missing dependencies, 
brew can be used to install the dependencies.

Build the project

```bash
bazel build --config=darwin heron/...  
```

Build the packages

```bash
bazel build --config=darwin scripts/packages:binpkgs  
bazel build --config=darwin scripts/packages:tarpkgs  
```

This will build the packages below the `bazel-bin/scripts/packages/` directory. 




