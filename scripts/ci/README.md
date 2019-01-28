# Heron CI scripts
This directory contains scripts used by CI jobs.

## Build release packages

Release packages include heron.tar.gz as well as installer script. Packages are platform dependent.

Example:

```bash
set -e
set -o pipefail

# Install bazel (linux build) because CI hosts may not have it installed
sh scripts/ci/setup_bazel.sh linux

# Build v0.20.1-incubating packages for centos7 and put in artifacts folder
HERON_BUILD_USER=release-agent
sh scripts/ci/build_release_packages.sh v0.20.1-incubating centos7 artifacts

```

## Build maven artifacts

Maven argifacts include api, spi, storm-compatibility and simulator. Artifacts are platform indepedent.

Example:

```bash
set -e
set -o pipefail

# Build v0.20.1-incubating artifacts and put in artifacts folder
HERON_BUILD_USER=release-agent
sh scripts/ci/build_maven_artifacts.sh v0.20.1-incubating artifacts

```
