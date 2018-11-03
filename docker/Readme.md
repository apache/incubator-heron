# Docker

## To compile source, into Heron release artifacts, using a docker container:
```
./docker/scripts/build-artifacts.sh <platform> <version_string> [source-tarball] <output-directory>
# e.g.  ./docker/scripts/build-artifacts.sh ubuntu14.04 testbuild ~/heron-release
```

## To build docker containers for running heron daemons:
```
./docker/scripts/build-docker.sh <platform> <version_string> <output-directory>
# e.g. ./docker/scripts/build-docker.sh ubuntu14.04 testbuild ~/heron-release
```
