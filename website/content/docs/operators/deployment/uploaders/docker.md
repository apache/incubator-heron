---
title: Setting Up Docker Uploader
---

Heron can be configured to create and upload Docker images to a configured
docker registry. Docker provides containerization for files and processes.
Topologies can leverage Docker containers to provide increased isolation for individual
topologies and to provide a clean mechanism to control the files available to each
topology.

The Docker Uploader is intended to to work with the Aurora Scheduler and Aurora's support
for containers. However the containers built by the uploader are simple enough to be leveraged
by other schedulers or serve as a base for a more complex container. The Uploader
generates a Docker image with 2 directives.

FROM *base-image*
ADD *topology-definition-files* /home/*role*/*topology-name*

The container is tagged as:
*registry*/*cluster*/*role*/*environ*/*topology-name*:*UUID*

For more information on Docker and how it can be used see:
[https://docs.docker.com/engine/understanding-docker/](https://docs.docker.com/engine/understanding-docker/)

The Docker uploader leverages Docker's command line client to build containers and push them to
remote registries. If the command line client isn't installed, or not configured then creation of
Docker containers will fail.

### Docker Uploader Configuration

You can make Heron use Docker uploader by modifying the `uploader.yaml` config file specific
for the Heron cluster. You'll need to specify the following for each cluster:

* `heron.class.uploader` --- Indicate the uploader class to be loaded. You should set this
to `com.twitter.heron.uploader.docker.DockerUploader`

* `heron.uploader.docker.base` --- The base image to use in the FROM directive this could
be something generic like `ubuntu:trusty` that is available on Docker hub or it could reference
a custom base image, for instance one with the Heron Core libraries installed.
 
* `heron.uploader.docker.registry` --- The registry to push the image to. May be omitted.
 
* `heron.uploader.docker.push` --- If true the uploader will invoke the `docker push` command
after building the image. This will push the image to the configured registry.

### Example Docker Uploader Configuration

Below is an example configuration (in `uploader.yaml`) for a Docker uploader:

```yaml
# uploader class for transferring the topology jar/tar files to storage
heron.class.uploader:         com.twitter.heron.uploader.docker.DockerUploader

#base image
heron.uploader.docker.base:                      ubuntu:latest

#registry prefix:
heron.uploader.docker.registry:                docker.example.com

#push
heron.uploader.docker.push:                      true
```

###Configuring Aurora

In order to leverage the Docker uploader with the Aurora scheduler you will need
to modify the `heron.aurora` job template, an example is included as `heron.docker.aurora`.
It leverages docker container support built into Aurora to pull the Docker image
on the executor. For more information about container support in Aurora please see the
Aurora documentation:
[http://aurora.apache.org/documentation/latest/features/containers/](http://aurora.apache.org/documentation/latest/features/containers/)
