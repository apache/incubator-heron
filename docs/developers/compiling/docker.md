# Compiling Heron with Docker

To use heron you will need to compile it for the environment that you want to use it in. Heron provides a convenient build script which uses docker to make this process easier. Currently only Ubuntu 14.04 and Centos7 are supported but if you need another platform feel free to add a Dockerfile for the specific environment you're targeting.

## Requirements

* [Docker](https://docs.docker.com)

## Recommendation

If you are running docker in a virtual machine, it is recommended that you adjust your settings to help speed up the build. To do this, open VirtualBox and go to the container which docker is running in (usually default or whatever name you used to create the VM). Click on the virtual machine and then click on settings. 

__NOTE__: You will need to stop the virtual machine before modifying these settings.

![VirtualBox Processors](img/virtual-box-processors.png)
![VirtualBox Memory](img/virtual-box-memory.png)

## Building Heron

Heron provides a build script for docker located here: `$ /path/to/heron/repo/docker/build-articats.sh`.

```
Usage: docker/build-artifacts.sh <platform> <version-string> [source-tarball] <output-directory>

Platforms: ubuntu14.04, centos7

Example:
  docker/build-artifacts.sh ubuntu14.04 0.1.0-SNAPSHOT ~/heron-release

NOTE: If running on OSX, the output directory will need to
      be under /Users so virtualbox has access to.
```

The `platform`, `version-string`, and `output-directory` arguments are required and you can optionally include a tarball of the heron source if you have one. By default the  script will create a tarball of the current source in the heron repo and use that to build the artifacts.

You can build a full Heron release for the runtime environment you're targeting, such as Ubuntu 14.04, by doing:

```
$ cd /path/to/heron/repo
$ docker/build-artifacts.sh ubuntu14.04 0.1.0-SNAPSHOT ~/heron-release
```

This will build a docker container specific to Ubuntu 14.04, create a source tarball of the heron repository, run a full release build of heron, then copy the artifacts into the ~/heron-release directory.

__NOTE__: If you are running on OSX, docker must be run inside a virtual machine. Therefore you must make sure that both the source tarball and destination directory are somewhere under your home directory. For example you cannot output the heron artifacts to /tmp because /tmp refers to the directory inside the VM not on the host machine. Your home directory however is automatically linked in to the VM and can be accessed normally.

After the build has completed you can go to your output directory and see all of the generated artifacts 
```bash
$ ls ~/heron-release
heron-0.1.0-SNAPSHOT.tar.gz
heron-api-0.1.0-SNAPSHOT.tar.gz
heron-bin-0.1.0-SNAPSHOT.tar.gz
heron-cli-0.1.0-SNAPSHOT.tar.gz
heron-conf-0.1.0-SNAPSHOT.tar.gz
heron-core-0.1.0-SNAPSHOT.tar.gz
# etc
```
