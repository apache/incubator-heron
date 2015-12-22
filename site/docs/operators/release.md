# Compiling a Heron Release

Heron is currently available for Mac OS X and CentOS 5.

You can build Heron either [as an entirety](#building-a-full-release) or on a
[component-by-component basis](#building-specific-components).

## Requirements

* [Bazel](http://bazel.io/docs/install.html) >= {{book.bazel_version}}
* [Java
  8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Autoconf](http://www.gnu.org/software/autoconf/autoconf.html) >= 2.6.3
* [Automake](https://www.gnu.org/software/automake/) >= 1.11.1
* [GNU Make](https://www.gnu.org/software/make/) >= 3.81
* [gcc/g++](https://gcc.gnu.org/) >= 4.8.2 (Linux platforms)
* [CMake](https://cmake.org/) >= 2.6-patch 4
* [Python](https://www.python.org/) >= 2.7
* [Perl](https://www.perl.org/) >= 5.8.8

## Configuring Bazel

[Bazel](http://bazel.io) is the build tool used by Heron. There is a Python
script that you can use to set up Bazel:

```bash
$ cd /path/to/heron/repo
$ ./bazel_configure.py
```

To ensure that Bazel has been installed, run `bazel version` and check the
version (listed next to `Build label` in the script's output) to ensure that you
have Bazel {{book.bazel_version}} or later.

## Building a Full Release

You can build a full Heron release using the `release:packages` [Bazel
target](http://bazel.io/docs/build-ref.html#targets). This will package all
Heron executables into `.tar` and `.tar.gz` files at once. You must specify the
following when building:

* A release name using the `RELEASE` parameter. For Bazel, this takes the form
  of `--define RELEASE={name}`.
* An OS-specific configuration using the `--config` flag, depending on the
  operating system. This can take one of the following values: `darwin` (Mac OS
  X), `centos5` (CentOS 5).

Here's an example build command:

```bash
$ cd /path/to/heron/repo
$ bazel build --define RELEASE=0.1.0-SNAPSHOT release:packages --config=darwin
```

### Build Directory

All `.tar` and `.tar.gz` files generated during Bazel's build process for a full
release can be found in `bazel-genfiles/release`:

```bash
$ ls bazel-genfiles/release
RELEASE
bazel-genfiles/release/heron-api-unversioned.tar
bazel-genfiles/release/heron-api-unversioned.tar.gz
# etc
```

## Running Unit Tests

You can run unit tests for Heron using the `bazel test` command.

You must specify an OS-specific configuration using the `--config` flag. This
can take one of the following values: `darwin` (Mac OS X), `centos5` (CentOS 5).

Here's an example test command:

```bash
$ bazel test heron/state/tests/java:local_file_state_manager_unittest
```

To fetch a full listing of test targets:

```bash
$ bazel query 'kind(".*_test rule", ...)'
```

For Java targets only:

```bash
$ bazel query 'kind("java_test rule", ...)'
```

For C++ targets:

```bash
$ bazel query 'kind("cpp_test rule", ...)'
```

For Python targets:

```bash
$ bazel query 'kind("pex_test rule", ...)'
```
