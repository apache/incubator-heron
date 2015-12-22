# Compiling a Heron Release

Heron is currently available for Mac OS X and Linux.

You can build Heron either [as an entirety](#building-a-full-release) or on a
[component-by-component basis](#building-specific-components).

## Requirements

* [Bazel](http://bazel.io/docs/install.html) >= 0.1.2
* [Java
  8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
* [Autoconf](http://www.gnu.org/software/autoconf/autoconf.html) >= 2.6.3
* [Automake](https://www.gnu.org/software/automake/) >= 1.11.1
* [GNU Make](https://www.gnu.org/software/make/) >= 3.81
* [gcc/g++](https://gcc.gnu.org/) >= 4.8.2 (Linux platforms)
* [CMake](https://cmake.org/) >= 2.6-patch 4
* [Python](https://www.python.org/) >= 2.7
* [Perl](https://www.perl.org/) >= 5.8.8

## Building a Full Release

You can build a full Heron release using the `release:packages` [Bazel
target](http://bazel.io/docs/build-ref.html#targets). This will package all
Heron executables into `.tar` and `.tar.gz` files at once. You must specify the
following when building:

* A release name using the `RELEASE` parameter. For Bazel, this takes the form
  of `--define RELEASE={name}`.
* A configuration using the `--config` flag.


```bash
$ cd /path/to/heron
$ ./bazel build --define RELEASE=0.1.0-SNAPSHOT release:packages
```

You'll need to have Python 2.x installed to build a Heron release. By default,
Heron will use whichever Python 2.x is available at `which python2`, but you
can specify a path for Python using the `--python2_path` flag. Here's an
example:

```bash
$ ./bazel build --python2_path /usr/bin/python2.7 RELEASE=0.1.0-SNAPSHOT release:packages
```

## Build Directory

All resulting `.tar` and `.tar.gz` files generated during the release build
process can be found in `bazel-genfiles/release`:

```bash
$ ls bazel-genfiles/release
RELEASE
bazel-genfiles/release/heron-api-unversioned.tar
bazel-genfiles/release/heron-api-unversioned.tar.gz
# etc
```

## Building Specific Components

The `build` command documented above produces `.tar` and `.tar.gz` files for all
Heron components. You can also build components one at a time. This, for
example, would build the [Heron Tracker](TODO):

```bash
$ ./bazel build --define RELEASE=0.1.0-SNAPSHOT release:tracker
```

The resulting tars would be stored in
`bazel-genfiles/release/heron-tracker-unversioned.{tar,tar.gz}`.

The following components can be built on their own:

* `api`
* `core`
* `cli`
* `metrics-api`
* `storm-compat`
* `tracker`
* `ui`
* `viz`
