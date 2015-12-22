# Creating a New Heron Release

All components necessary for a Heron cluster can be easily packaged into a
series of `.tar` and `.tar.gz` files using the [`bazel`](http://bazel.io/)
executable located at the root of the Heron repository.

Generating a full release can be useful for hosting Heron binaries on your own
infrastructure or using a public object storage tool like [Amazon
S3](https://aws.amazon.com/s3/).

## Building a Full Release for All Components

The `release:packages` Bazel target enables you to package all Heron components
into `.tar` and `.tar.gz` files at once.  You must specify a release name when
building. Here's an example `build` command:

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
